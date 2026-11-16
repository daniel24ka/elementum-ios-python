"""Rolling buffer, seek priority, source ranking and stall recovery contracts."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
import importlib.util
import threading
import types
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'addon/resources/lib'))
from elementum_py.core import Metainfo, PieceStore, bencode
from elementum_py.playback_policy import StallGuard, read_ahead_pieces, kodi_cache_target
from elementum_py.source_health import SourceHealth, identity
from elementum_py.backend import Backend
from test_backend import FakeKodi, FakeEngine, catalog_transport
from elementum_py.catalog import Catalog

class BufferTests(unittest.TestCase):
    def setUp(self):
        self.length=16384
        self.payload=bytes(range(256))*8192
        info={b'name':b'film.mp4',b'length':len(self.payload),b'piece length':self.length,
              b'pieces':b''.join(hashlib.sha1(self.payload[i:i+self.length]).digest() for i in range(0,len(self.payload),self.length))}
        self.meta=Metainfo(bencode({b'info':info}))
        self.calls=[]
        def fetch(index):
            self.calls.append(index)
            time.sleep(.003)
            return self.payload[index*self.length:(index+1)*self.length]
        self.store=PieceStore(self.meta,max_cache_bytes=self.length*12,peer_fetcher=fetch,allow_webseeds=False)
        self.store.configure_stream(0)
        self.store.update_playback(128)
    def tearDown(self):
        self.store.close()
    def wait(self,condition):
        deadline=time.monotonic()+3
        while not condition() and time.monotonic()<deadline:
            time.sleep(.01)
        self.assertTrue(condition())
    def test_background_window_continues_beyond_initial_three_jobs(self):
        reader=self.store.read(0,0,len(self.payload))
        self.assertEqual(next(reader),self.payload[:self.length])
        self.wait(lambda: all(i in self.store.cache for i in range(1,11)))
        self.assertGreaterEqual(self.store.buffer_stats()['ahead_verified_bytes'],10*self.length)
        self.assertLessEqual(sum(map(len,self.store.cache.values())),12*self.length)
        reader.close()
    def test_seek_replaces_window_and_old_reader_cannot_retarget_it(self):
        old=self.store.read(0,0,len(self.payload)); next(old)
        seek=self.store.read(0,1024*1024,512*1024); next(seek)
        generation=self.store.window_generation
        next(old)
        self.assertEqual(self.store.window_generation,generation)
        self.assertTrue(all(i>=65 for i in self.store.window))
        self.wait(lambda: all(i in self.store.cache for i in self.store.window))
        old.close(); seek.close()
    def test_small_codec_tail_probe_does_not_replace_stream_window(self):
        reader=self.store.read(0,0,len(self.payload)); next(reader)
        generation=self.store.window_generation
        self.assertEqual(b''.join(self.store.read(0,len(self.payload)-100,100)),self.payload[-100:])
        self.assertEqual(self.store.window_generation,generation)
        reader.close()
    def test_large_open_ended_codec_probe_restores_live_reader_on_close(self):
        reader=self.store.read(0,0,len(self.payload)); next(reader)
        original=self.store.window_generation
        probe=self.store.read(0,len(self.payload)-300000,300000); next(probe)
        self.assertNotEqual(self.store.window_generation,original)
        probe.close()
        self.assertEqual(self.store.window_generation,original)
        self.assertLess(self.store.window[0],10)
        self.wait(lambda:all(i in self.store.cache for i in self.store.window))
        reader.close()
    def test_last_piece_stops_prefetch_at_selected_file_boundary(self):
        reader=self.store.read(0,len(self.payload)-262144,262144)
        self.assertEqual(b''.join(reader),self.payload[-262144:])
        self.assertEqual(self.store.window,())
        self.assertTrue(all(index<len(self.meta.hashes) for index in self.calls))
    def test_close_joins_rolling_scheduler_and_cancels_prefetch(self):
        reader=self.store.read(0,0,len(self.payload)); next(reader)
        self.store.close()
        self.assertFalse(self.store.readahead_thread.is_alive())
        self.assertTrue(self.store.cancel.is_set())
        reader.close()
    def test_time_target_is_bounded_for_large_and_unknown_bitrates(self):
        self.assertEqual(read_ahead_pieces(64*1024*1024,1,10*1024*1024),0)
        self.assertEqual(read_ahead_pieces(1024*1024,32,100*1024*1024),30)
        self.assertGreater(read_ahead_pieces(1024*1024,64,1024*1024,100),
                           read_ahead_pieces(1024*1024,64,1024*1024,10*1024*1024))
    def test_native_cache_target_preserves_custom_and_disk_choices(self):
        self.assertEqual(kodi_cache_target(654,20),64)
        self.assertEqual(kodi_cache_target(300,20),32)
        self.assertEqual(kodi_cache_target(100,20),20)
        self.assertEqual(kodi_cache_target(654,128),128)
        self.assertEqual(kodi_cache_target(654,0),0)

class HealthTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory(); self.now=1000000
        self.health=SourceHealth(self.folder.name,lambda:self.now)
    def tearDown(self): self.folder.cleanup()
    def test_history_reorders_only_within_existing_quality_slots(self):
        sources=[{'uri':'a','name':'Film 1080p'},{'uri':'b','name':'Film 2160p'},{'uri':'c','name':'Film 1080p'}]
        self.health.record('a',False); self.health.record('c',True,elapsed=3,rate=2000000)
        ranked=self.health.rank(sources)
        self.assertEqual([item['uri'] for item in ranked],['c','b','a'])
    def test_history_persists_no_urls_or_passkeys(self):
        uri='https://example.test/private/PASSKEY/movie.torrent?token=SECRET'
        self.health.record(uri,False)
        text=Path(self.health.path).read_text()
        self.assertNotIn('PASSKEY',text); self.assertNotIn('SECRET',text); self.assertNotIn('https',text)
        loaded=SourceHealth(self.folder.name,lambda:self.now)
        self.assertLess(loaded.score(uri),0)
    def test_history_is_bounded_and_expires(self):
        for i in range(260): self.health.record(str(i),True)
        self.assertLessEqual(len(self.health.entries),256)
        self.now+=8*86400
        self.assertEqual(self.health.score('259'),0)
        self.assertEqual(SourceHealth(self.folder.name,lambda:self.now).entries,{})
    def test_same_info_hash_ignores_different_tracker_passkeys(self):
        a='magnet:?xt=urn:btih:'+'a'*40+'&tr=https://tracker.test/key1'
        b='magnet:?xt=urn:btih:'+'a'*40+'&tr=https://tracker.test/key2'
        self.assertEqual(identity(a),identity(b))
    def test_unknown_sources_keep_provider_order(self):
        sources=[{'uri':str(i),'name':'1080p'} for i in range(8)]
        self.assertEqual(self.health.rank(sources),sources)

class StallTests(unittest.TestCase):
    def test_persistent_empty_buffer_stall_triggers_once(self):
        guard=StallGuard()
        self.assertFalse(guard.sample('a',90,600,1000,0))
        self.assertTrue(guard.sample('a',90,600,1000,26,caching=True))
        self.assertFalse(guard.sample('a',90,600,1000,70,caching=True))
    def test_pause_seek_and_active_transfer_prevent_false_recovery(self):
        for kind in ('pause','seek','transfer'):
            guard=StallGuard(); guard.sample('a',90,600,1000,0)
            values={'paused':kind=='pause','caching':True}
            self.assertFalse(guard.sample('a',130 if kind=='seek' else 90,600,
                                         2000 if kind=='transfer' else 1000,26,**values))
    def test_no_stall_without_caching_or_at_end_or_new_url(self):
        guard=StallGuard(); guard.sample('a',90,600,1,0)
        self.assertFalse(guard.sample('a',90,600,1,80))
        self.assertFalse(guard.sample('a',590,600,1,81,caching=True,failed=True))
        self.assertFalse(guard.sample('b',90,600,1,100,caching=True))

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory(); self.kodi=FakeKodi(); self.engine=FakeEngine()
        self.backend=Backend(self.folder.name,self.kodi,engine=self.engine)
        self.sources=[{'uri':'magnet:?xt=urn:btih:'+letter*40,'name':'Film '+res}
                      for letter,res in (('a','1080p'),('b','2160p'),('c','1080p'),('d','720p'))]
        self.query={'tmdb':'81747','type':'movie'}
        self.session={'uri':self.sources[0]['uri'],'title':'Film','playback_query':self.query,'recovery_chain':'token'}
        self.engine.active=self.session
        self.backend.recovery_chains['token']={'attempted':{'a'*40},'count':0,'when':time.monotonic()}
        self.backend.remember_sources('Film',self.query,self.sources)
    def tearDown(self):
        self.backend.downloads.close(); self.folder.cleanup()
    def test_recovery_skips_higher_resolution_and_preserves_exact_position(self):
        request=self.backend.recovery_request(self.session,352.75)
        query=parse_qs(urlsplit(request).query)
        self.assertEqual(query['uri'],[self.sources[2]['uri']])
        self.assertEqual(query['fallback'],['0'])
        self.assertEqual(self.backend.recovery_chains['token']['position'],352.75)
    def test_recovery_is_bounded_to_two_distinct_alternatives(self):
        self.assertIsNotNone(self.backend.recovery_request(self.session,90))
        self.assertIsNotNone(self.backend.recovery_request(self.session,90))
        self.assertIsNone(self.backend.recovery_request(self.session,90))
    def test_recovery_never_guesses_a_title_for_raw_magnet(self):
        self.session['playback_query']={}
        self.assertIsNone(self.backend.recovery_request(self.session,90))
    def test_recovery_honors_disable_cancel_and_replaced_session(self):
        self.kodi.setting=lambda key:'false' if key=='playback_recovery' else ''
        self.assertIsNone(self.backend.recovery_request(self.session,90))
        self.kodi.setting=lambda key:''; self.kodi.abort_requested=lambda:True
        self.assertIsNone(self.backend.recovery_request(self.session,90))
        self.kodi.abort_requested=lambda:False; self.engine.active=None
        self.assertIsNone(self.backend.recovery_request(self.session,90))

    def test_resolved_replacement_consumes_chain_resume_position_without_prompt(self):
        request=self.backend.recovery_request(self.session,352.75)
        query={key:values[0] for key,values in parse_qs(urlsplit(request).query).items()}
        self.backend.catalog=Catalog(self.folder.name+'/catalog',transport=catalog_transport)
        def prepare(uri,selected,title,peer_only=False):
            self.engine.active={'stream':types.SimpleNamespace(url='http://127.0.0.1/new'), 'uri':uri}
            return {'url':'http://127.0.0.1/new','title':title,'file_index':0}
        self.engine.prepare=prepare
        self.backend.play(query)
        self.assertEqual(self.backend.pending_resume,352.75)
        self.assertEqual(self.engine.active['recovery_chain'],'token')
        self.assertEqual(self.backend.last_recovery['midstream']['attempt'],1)

class ServiceRecoveryTests(unittest.TestCase):
    def exercise(self,mode):
        state={'clock':0,'loops':0,'url':'http://127.0.0.1/old','opens':[],'seeks':[],'errors':[],'stops':0}
        store=types.SimpleNamespace(network_bytes=1000,update_playback=lambda *args:None)
        session={'stream':types.SimpleNamespace(url=state['url']),'store':store,'uri':'magnet:?fixture'}
        engine=types.SimpleNamespace(active=session)
        def stop():
            state['stops']+=1; engine.active=None
        engine.stop=stop
        backend=types.SimpleNamespace(engine=engine,shutdown=threading.Event(),pending_resume=0,
            state={'history':[{}]},source_health=types.SimpleNamespace(record=lambda *args,**kwargs:None),
            announce_next_episode=lambda *args:None,save=lambda:None)
        backend.recovery_request=lambda session,position:'plugin://plugin.video.elementum/play?fixture'
        class Monitor:
            def waitForAbort(self,seconds):
                state['loops']+=1; state['clock']+=2
                return state['loops']>25
        class Player:
            def isPlayingVideo(self): return not(mode=='stop' and state['loops']>=5)
            def getPlayingFile(self): return state['url']
            def getTime(self): return 90
            def getTotalTime(self): return 600
            def seekTime(self,value): state['seeks'].append(value)
        class Kodi:
            dialog=None
            def __init__(self,*args): pass
            def setting(self,key): return 'false'
            def clear_ids(self,*args): pass
            def publish_ids(self,*args): return {}
            def notify(self,*args): pass
            def progress_close(self): pass
            def error(self,*args): state['errors'].append(args)
            def rpc(self,method,params):
                state['opens'].append(params)
                state['url']='http://127.0.0.1/new'
                engine.active={'stream':types.SimpleNamespace(url=state['url']),'store':store,'uri':'magnet:?replacement'}
                backend.pending_resume=90
        stubs={'xbmc':types.SimpleNamespace(Player=Player,Monitor=Monitor,LOGINFO=1,log=lambda *args:None,
                    getCondVisibility=lambda key:mode=='pause' if key=='Player.Paused' else state['url'].endswith('old')),
               'xbmcaddon':types.SimpleNamespace(Addon=lambda *args:types.SimpleNamespace(getAddonInfo=lambda key:'')),
               'xbmcgui':types.SimpleNamespace(),'xbmcvfs':types.SimpleNamespace(translatePath=lambda path:path)}
        name='elementum_py._service_recovery_test'
        path=Path(sys.modules['elementum_py.backend'].__file__).with_name('kodi_service.py')
        spec=importlib.util.spec_from_file_location(name,path); module=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,stubs):
            spec.loader.exec_module(module)
            with patch.object(module,'Kodi',Kodi),patch.object(module,'Backend',lambda *args:backend),\
                 patch.object(module,'BackendServer',lambda *args:types.SimpleNamespace(close=lambda:None)),\
                 patch.object(module,'time',types.SimpleNamespace(monotonic=lambda:state['clock'])):
                module.run()
        self.assertEqual(state['errors'],[])
        return state
    def test_real_service_loop_reopens_stalled_stream_and_seeks_after_resolution(self):
        state=self.exercise('stall')
        self.assertEqual(len(state['opens']),1)
        self.assertEqual(state['seeks'],[90])
        self.assertEqual(state['stops'],0)
    def test_real_service_loop_never_recovers_a_user_pause_or_stop(self):
        for mode in ('pause','stop'):
            state=self.exercise(mode)
            self.assertEqual(state['opens'],[])

if __name__=='__main__': unittest.main(verbosity=2)

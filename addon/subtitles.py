"""Kodi's xbmc.subtitle.module entry point for Elementum."""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'resources', 'lib'))
from elementum_py.subtitle_bridge.service import main
main()

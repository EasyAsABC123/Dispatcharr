"""Exact release/default/Info isolation regressions; no cfg(test) peer inference."""
import copy,re,unittest
from pathlib import Path
R=Path(__file__).resolve().parent/'source/rust/crates/xc-http/src'
MODULES=('vod_json','vod_rating','vod_lists','vod_priority','vod_properties','vod_span')
def require(ok,label):
 if not ok:raise ValueError(label)
def validate(lib,gateway,project):
 a=gateway.index('} else if self.accepts_vod_list(action) {');b=gateway.index('} else if matches!(',a);block=gateway[a:b]
 require('#[cfg('not in block and 'crate::vod_lists::write('in block and 'XC VOD list staging is test-only'not in block,'ReleaseListWriterMustActuallyCompile')
 require('vod_lists_enabled: AtomicBool::new(false)'in gateway and 'pub fn with_vod_lists(self)'in gateway,'LibraryDefaultOffExplicitCapability')
 info=gateway[gateway.index('fn accepts_vod_info'):gateway.index('fn accepts_vod_list')]
 require('#[cfg(test)]'in info and '#[cfg(not(test))]'in info and 'false'in info,'InfoRemainsTestOnly')
 for name in MODULES:require('mod '+name+';'in lib and '#[cfg(test)]\nmod '+name+';'not in lib,'ReleasePrimitive:'+name)
 for name in ('vod_object','vod_info_stage','vod_info_refresh','vod_info_row','vod_movie_info','vod_episode_rows','vod_scalar','vod_seasons','vod_series_info'):require('#[cfg(test)]\nmod '+name+';'in lib,'InfoModuleStillGated:'+name)
 require('pub(crate) fn maximum(&self)'in project and '#[cfg(test)]\n    pub(crate) fn maximum'not in project,'ActualReleaseCapGetter')
 return True
class Controls(unittest.TestCase):
 def setUp(self):self.lib=(R/'lib.rs').read_text();self.gateway=(R/'gateway.rs').read_text();self.project=(R/'project.rs').read_text()
 def test_exact_production_path(self):self.assertTrue(validate(self.lib,self.gateway,self.project))
 def test_cfgtest_writer_regression(self):
  bad=self.gateway.replace('} else if self.accepts_vod_list(action) {','} else if self.accepts_vod_list(action) {\n#[cfg(test)]')
  with self.assertRaises(ValueError):validate(self.lib,bad,self.project)
 def test_default_unconditional_enable(self):
  with self.assertRaises(ValueError):validate(self.lib,self.gateway.replace('vod_lists_enabled: AtomicBool::new(false)','vod_lists_enabled: AtomicBool::new(true)'),self.project)
 def test_info_ungated(self):
  with self.assertRaises(ValueError):validate(self.lib.replace('#[cfg(test)]\nmod vod_info_stage;','mod vod_info_stage;'),self.gateway,self.project)
 def test_missing_release_primitive(self):
  for name in MODULES:
   with self.subTest(name=name),self.assertRaises(ValueError):validate(self.lib.replace('mod '+name+';','#[cfg(test)]\nmod '+name+';'),self.gateway,self.project)
 def test_cap_getter_test_only(self):
  with self.assertRaises(ValueError):validate(self.lib,self.gateway,self.project.replace('    pub(crate) fn maximum','    #[cfg(test)]\n    pub(crate) fn maximum'))
if __name__=='__main__':unittest.main(verbosity=2)

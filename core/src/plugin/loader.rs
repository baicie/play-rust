use crate::error::Result;
use std::path::Path;

pub struct PluginLoader {
    // 插件加载路径
    plugin_path: Option<String>,
}

impl PluginLoader {
    pub fn new() -> Self {
        Self { plugin_path: None }
    }

    pub fn set_plugin_path<P: AsRef<Path>>(&mut self, path: P) {
        self.plugin_path = Some(path.as_ref().to_string_lossy().into_owned());
    }

    // TODO: 实现动态加载插件的功能
    pub fn load_plugin(&self, _name: &str) -> Result<()> {
        Ok(())
    }
}

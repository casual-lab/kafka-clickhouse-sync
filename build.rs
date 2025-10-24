// build.rs - 编译 Protobuf 定义

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 编译 Protobuf 文件
    let mut config = prost_build::Config::new();
    
    // 为所有消息添加 serde 支持
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    
    // 设置 protoc 参数支持 proto3 optional 字段
    config.protoc_arg("--experimental_allow_proto3_optional");
    
    // 编译 proto 文件
    config.compile_protos(&["proto/event.proto"], &["proto/"])?;
    
    Ok(())
}

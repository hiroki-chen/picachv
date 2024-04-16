use jni::{objects::JClass, JNIEnv};

#[no_mangle]
pub extern "system" fn build_logical_node_java<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
) -> i32 {
    0
}

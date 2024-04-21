use jni::{
    objects::{JByteArray, JClass},
    JNIEnv,
};

type Callback<'local> =
    unsafe extern "system" fn(JNIEnv<'local>, JClass<'local>, JByteArray) -> i32;

struct CallbackWrapper<'local> {
    cb: Callback<'local>,
}

#[no_mangle]
pub extern "system" fn build_logical_node_java<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
) -> i32 {
    0
}

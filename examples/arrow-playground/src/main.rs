use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::ipc::writer::StreamWriter;

fn main()  {
    let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as _;
    let rb = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();

    let v = vec![];
    let mut ipc_writer = StreamWriter::try_new(v, rb.schema().as_ref()).unwrap();
    ipc_writer.write(&rb).unwrap();

    ipc_writer.finish().unwrap();

    println!("serialized arrow record batch {:?}", ipc_writer.get_ref());

    let res = ipc_writer.get_ref().clone();

    let mut ipc_reader = arrow::ipc::reader::StreamReader::try_new(&res[..], None).unwrap();
    let rb = ipc_reader.next().unwrap().unwrap();
    println!("deserialized arrow record batch {:?}", rb);
}
pub(crate) mod arrow_utils;
pub mod error;

#[macro_export]
macro_rules! arrow_downcast {
    ($array:expr, $ty:ty) => {
        $array.as_any().downcast_ref::<$ty>().unwrap()
    };
}

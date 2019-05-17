#[macro_use]
extern crate neon;
extern crate neon_serde;
extern crate carmen_core;
use neon::prelude::*;
use carmen_core::gridstore::{GridStoreBuilder, GridEntry, GridKey};

trait CheckArgument {
    fn check_argument<V: Value>(&mut self, i: i32) -> JsResult<V>;
}

declare_types! {
    pub class JsGridStoreBuilder as JsGridStoreBuilder for Option<GridStoreBuilder> {
        init(mut cx) {
            let filename = cx
                .argument::<JsString>(0)
                ?.value();
            match GridStoreBuilder::new(filename) {
                Ok(s) => Ok(Some(s)),
                Err(e) => cx.throw_type_error(e.description())
            }
        }
        method insert(cx) {
            let grid_key = cx.argument::<JsValue>(0)?;
            let grid_entry = cx.argument::<JsValue>(1)?;
            let mut this: Handle<JsGridStoreBuilder> = cx.argument.this(cx);
            let entry: Vec<GridEntry> = neon_serde::from_value(&mut cx, grid_entry)?;
            let key: Vec<GridKey> = neon_serde::from_value(&mut cx, grid_key)?;

        }
    }
}

register_module!(mut m, {
    m.export_class::<JsGridStoreBuilder>("JsGridStoreBuilder")
});

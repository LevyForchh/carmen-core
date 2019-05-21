#[macro_use]
extern crate neon;
extern crate neon_serde;
extern crate carmen_core;
use neon::prelude::*;
use carmen_core::gridstore::{GridStoreBuilder, GridEntry, GridKey};

trait take {
    fn take<V: Value>(&mut self, i: i32) -> JsResult<V>;
}
pub fn take(&mut self) -> Option<T> {
       mem::replace(self, None)
   }

impl

declare_types! {
    pub class JsGridStoreBuilder as JsGridStoreBuilder for GridStoreBuilder {
        init(mut cx) {
            let filename = cx
                .argument::<JsString>(0)
                ?.value();
            match GridStoreBuilder::new(filename) {
                Ok(s) => Ok(s),
                Err(e) => cx.throw_type_error(e.description())
            }
        }

        method insert(mut cx) {
            let grid_key = cx.argument::<JsValue>(0)?;
            let grid_entry = cx.argument::<JsValue>(1)?;
            let values: Vec<GridEntry> = neon_serde::from_value(&mut cx, grid_entry)?;
            let key: GridKey = neon_serde::from_value(&mut cx, grid_key)?;
            let mut this = cx.this();

            // lock falls out of scope at the end of this block
            // in order to be able to borrow `cx` for the error block we assign it to a variable
            let insert = {
                let lock = cx.lock();
                let mut gridstore = this.borrow_mut(&lock);
                gridstore.insert(&key, &values)
            };

            match insert {
                Ok(_) => Ok(JsUndefined::new().upcast()),
                Err(e) => cx.throw_type_error(e.description())
            }
        }

        method finish(mut cx) {
            let this = cx.this();

            let finish = {
                let lock = cx.lock();
                let gridstore = this.borrow(&lock);
                gridstore.take().finish()
            };

            match finish {
                Ok(_) => Ok(JsUndefined::new().upcast()),
                Err(e) => cx.throw_type_error(e.description())
            }
        }
    }
}


register_module!(mut m, {
    m.export_class::<JsGridStoreBuilder>("JsGridStoreBuilder")
});

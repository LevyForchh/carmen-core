#[macro_use]
extern crate neon;
extern crate carmen_core;
extern crate neon_serde;
use carmen_core::gridstore::{GridEntry, GridKey, GridStoreBuilder};
use neon::prelude::*;
use std::error::Error;

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

        method insert(mut cx) {
            let grid_key = cx.argument::<JsValue>(0)?;
            let grid_entry = cx.argument::<JsValue>(1)?;
            let values: Vec<GridEntry> = neon_serde::from_value(&mut cx, grid_entry)?;
            let key: GridKey = neon_serde::from_value(&mut cx, grid_key)?;
            let mut this = cx.this();

            // lock falls out of scope at the end of this block
            // in order to be able to borrow `cx` for the error block we assign it to a variable

            let insert: Result<Result<(), Box<dyn Error>>, &str> = {
                let lock = cx.lock();
                let mut gridstore = this.borrow_mut(&lock);
                match gridstore.as_mut() {
                    Some(builder) => {
                        Ok(builder.insert(&key, &values))
                    }
                    None => {
                        Err("unable to insert()")
                    }
                }
            };

            match insert {
                Ok(_) => Ok(JsUndefined::new().upcast()),
                Err(e) => cx.throw_type_error(e)
            }
        }

        method finish(mut cx) {
            let mut this = cx.this();

            let finish: Result<Result<(), Box<dyn Error>>, &str> = {
                let lock = cx.lock();
                let mut gridstore = this.borrow_mut(&lock);
                match gridstore.take() {
                    Some(builder) => {
                        Ok(builder.finish())
                    }
                    None => {
                        Err("unable to finish()")
                    }
                }
            };

            match finish {
                Ok(_) => Ok(JsUndefined::new().upcast()),
                Err(e) => cx.throw_type_error(e)
            }
        }
    }
}

register_module!(mut m, { m.export_class::<JsGridStoreBuilder>("JsGridStoreBuilder") });

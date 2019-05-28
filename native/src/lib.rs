#[macro_use]
extern crate neon;
extern crate carmen_core;
extern crate neon_serde;
use carmen_core::gridstore::{GridEntry, GridKey, GridStore, GridStoreBuilder};
use neon_serde::errors::Result as LibResult;
use neon::prelude::*;
use std::error::Error;

declare_types! {
    pub class JsGridStoreBuilder as JsGridStoreBuilder for Option<GridStoreBuilder> {
        init(mut cx) {
            let filename = cx.argument::<JsString>(0)?.value();
            match GridStoreBuilder::new(filename) {
                Ok(s) => Ok(Some(s)),
                Err(e) => cx.throw_type_error(e.description())
            }
        }

        method insert(mut cx) {
            let grid_key = cx.argument::<JsObject>(0)?;
            let grid_entry = cx.argument::<JsValue>(1)?;
            let values: Vec<GridEntry> = neon_serde::from_value(&mut cx, grid_entry)?;
            let js_phrase_id = {
                grid_key.get(&mut cx, "phrase_id")?
            };
            let phrase_id: u32 = {
                js_phrase_id.downcast::<JsNumber>().or_throw(&mut cx)?.value() as u32
            };
            let js_lang_set = {
                grid_key.get(&mut cx, "lang_set")?
            };

            let js_array_lang = {
                js_lang_set.downcast::<JsArray>().or_throw(&mut cx)?
            };

            let lang_set: u128 = langarray_to_langset(&mut cx, js_array_lang)?;

            let key = GridKey { phrase_id, lang_set };
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

    pub class JsGridStore as JsGridStore for GridStore {
        init(mut cx) {
            let filename = cx.argument::<JsString>(0)?.value();
            match GridStore::new(filename) {
                Ok(s) => Ok(s),
                Err(e) => cx.throw_type_error(e.description())
            }
        }
    }
}

fn langarray_to_langset<'j, C>(cx: &mut C, lang_array: Handle<'j, JsArray>) -> LibResult<u128>
where
    C: Context<'j>
{
    let mut out = 0u128;
    for i in 0..lang_array.len() {
        out = out | (1 << lang_array.get(cx, i)?.downcast::<JsNumber>().or_throw(cx)?.value() as usize);
    }
    Ok(out)
}

pub fn coalesce(mut cx, FunctionContext) -> JsResult<JsUndefined> {

}

/*
fn langfield_to_langarray(&mut cx: FunctionContext, langset: u128) -> JsResult<JsArray> {
    let lang_array: Handle<JsArray> = JsArray::new(&mut cx, 0u32);
    let mut idx = 0u32;
    for i in 0..128 {
        if (langset & (1 << i)) != 0 {
            let js_number = cx.number(i as f64);
            lang_array.set(cx, idx, js_number);
            idx += 1;
        }
    }
    Ok(lang_array)
}
*/
register_module!(mut m, {
    m.export_class::<JsGridStoreBuilder>("JsGridStoreBuilder")?;
    m.export_class::<JsGridStore>("JsGridStore")?;
    Ok(())
});

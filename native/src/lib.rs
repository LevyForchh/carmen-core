#[macro_use]
extern crate neon;
extern crate carmen_core;
extern crate neon_serde;
use carmen_core::gridstore::coalesce;
use carmen_core::gridstore::PhrasematchSubquery;
use carmen_core::gridstore::{
    CoalesceContext, GridEntry, GridKey, GridStore, GridStoreBuilder, MatchOpts,
};

use neon::prelude::*;
use neon_serde::errors::Result as LibResult;
use std::error::Error;
use std::sync::Arc;

type ArcGridStore = Arc<GridStore>;

struct CoalesceTask {
    argument: (Vec<PhrasematchSubquery<ArcGridStore>>, MatchOpts),
}

impl Task for CoalesceTask {
    type Output = Vec<CoalesceContext>;
    type Error = String;
    type JsEvent = JsArray;

    fn perform(&self) -> Result<Vec<CoalesceContext>, String> {
        coalesce(self.argument.0.clone(), &self.argument.1).map_err(|err| err.to_string())
    }

    fn complete<'a>(
        self,
        mut cx: TaskContext<'a>,
        result: Result<Vec<CoalesceContext>, String>,
    ) -> JsResult<JsArray> {
        let converted_result = {
            match &result {
                Ok(r) => r,
                Err(s) => return cx.throw_error(s),
            }
        };
        Ok(neon_serde::to_value(&mut cx, converted_result)?
            .downcast::<JsArray>()
            .or_throw(&mut cx)?)
    }
}

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

    pub class JsGridStore as JsGridStore for ArcGridStore {
        init(mut cx) {
            let filename = cx.argument::<JsString>(0)?.value();
            match GridStore::new(filename) {
                Ok(s) => Ok(Arc::new(s)),
                Err(e) => cx.throw_type_error(e.description())
            }
        }
    }
}

fn langarray_to_langset<'j, C>(cx: &mut C, lang_array: Handle<'j, JsArray>) -> LibResult<u128>
where
    C: Context<'j>,
{
    let mut out = 0u128;
    for i in 0..lang_array.len() {
        out = out
            | (1 << lang_array.get(cx, i)?.downcast::<JsNumber>().or_throw(cx)?.value() as usize);
    }
    Ok(out)
}

pub fn js_coalesce(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let js_phrase_subq = { cx.argument::<JsArray>(0)? };
    let js_match_ops = { cx.argument::<JsValue>(1)? };
    let phrase_subq: Vec<PhrasematchSubquery<ArcGridStore>> =
        deserialize_phrasesubq(&mut cx, js_phrase_subq)?;
    let match_opts: MatchOpts = neon_serde::from_value(&mut cx, js_match_ops)?;
    let cb = cx.argument::<JsFunction>(2)?;

    let task = CoalesceTask { argument: (phrase_subq, match_opts) };
    task.schedule(cb);

    Ok(cx.undefined())
}

fn deserialize_phrasesubq<'j, C>(
    cx: &mut C,
    js_phrase_subq_array: Handle<'j, JsArray>,
) -> LibResult<Vec<PhrasematchSubquery<ArcGridStore>>>
where
    C: Context<'j>,
{
    let mut phrasematches: Vec<PhrasematchSubquery<ArcGridStore>> =
        Vec::with_capacity(js_phrase_subq_array.len() as usize);
    for i in 0..js_phrase_subq_array.len() {
        let js_phrasematch =
            js_phrase_subq_array.get(cx, i)?.downcast::<JsObject>().or_throw(cx)?;
        let js_gridstore =
            js_phrasematch.get(cx, "store")?.downcast::<JsGridStore>().or_throw(cx)?;
        let gridstore = {
            let guard = cx.lock();
            // shallow clone of the Arc
            let gridstore_clone = js_gridstore.borrow(&guard).clone();
            gridstore_clone
        };
        let weight = js_phrasematch.get(cx, "weight")?;
        let match_key = js_phrasematch.get(cx, "match_key")?;
        let idx = js_phrasematch.get(cx, "idx")?;
        let zoom = js_phrasematch.get(cx, "zoom")?;
        let mask = js_phrasematch.get(cx, "mask")?;

        let subq = PhrasematchSubquery {
            store: gridstore,
            weight: neon_serde::from_value(cx, weight)?,
            match_key: neon_serde::from_value(cx, match_key)?,
            idx: neon_serde::from_value(cx, idx)?,
            zoom: neon_serde::from_value(cx, zoom)?,
            mask: neon_serde::from_value(cx, mask)?,
        };
        phrasematches.push(subq);
    }
    Ok(phrasematches)
}

register_module!(mut m, {
    m.export_class::<JsGridStoreBuilder>("JsGridStoreBuilder")?;
    m.export_class::<JsGridStore>("JsGridStore")?;
    m.export_function("coalesce", js_coalesce)?;
    Ok(())
});

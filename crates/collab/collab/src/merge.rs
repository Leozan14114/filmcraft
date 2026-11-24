//! Three-way merge of JSON values: the heart of concurrent editing.
//!
//! A client that has local changes to a key when somebody else's change to the same key arrives
//! holds three values: `base` (the key as both started from), `mine` (the local value) and `theirs`
//! (the value the relay accepted). [`merge3`] keeps both sides' work wherever they did not touch the
//! same thing:
//!
//! - objects merge field by field (recursively);
//! - arrays whose elements are all objects with a distinct scalar `id` (clips, markers,
//!   transitions) merge by id: added, removed and changed elements are each kept, and an element
//!   both sides changed merges field by field;
//! - everything else is a leaf: if only one side changed it, that change wins; if both changed it
//!   differently, **theirs wins and the clash is counted**, so the caller can tell the user.
//!
//! Deleting an element one side left alone and the other removed removes it. Deleting an element the
//! other side changed keeps the changed element (and counts a clash): a change is never dropped
//! silently.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde_json::{Map, Value};

/// Deepest nesting merged field by field; deeper values are merged as one leaf.
const MAX_DEPTH: usize = 48;

/// The result of a merge.
#[derive(Clone, Debug, PartialEq)]
pub struct Merged {
    /// The merged value (`None`: the key is deleted).
    pub value: Option<Value>,
    /// How many leaves both sides changed differently (those took `theirs`).
    pub conflicts: usize,
}

/// Merge `mine` and `theirs`, which both descend from `base`.
pub fn merge3(base: Option<&Value>, mine: Option<&Value>, theirs: Option<&Value>) -> Merged {
    let mut conflicts = 0;
    let value = merge_inner(base, mine, theirs, &mut conflicts, 0);
    Merged { value, conflicts }
}

fn merge_inner(base: Option<&Value>, mine: Option<&Value>, theirs: Option<&Value>, conflicts: &mut usize, depth: usize) -> Option<Value> {
    if mine == theirs {
        return mine.cloned();
    }
    if mine == base {
        return theirs.cloned();
    }
    if theirs == base {
        return mine.cloned();
    }
    if depth < MAX_DEPTH {
        if let (Some(Value::Object(b)), Some(Value::Object(m)), Some(Value::Object(t))) = (base, mine, theirs) {
            return Some(Value::Object(merge_objects(b, m, t, conflicts, depth + 1)));
        }
        if let (Some(Value::Array(b)), Some(Value::Array(m)), Some(Value::Array(t))) = (base, mine, theirs)
            && let Some(merged) = merge_id_arrays(b, m, t, conflicts, depth + 1)
        {
            return Some(Value::Array(merged));
        }
    }
    *conflicts += 1;
    theirs.cloned()
}

fn merge_objects(base: &Map<String, Value>, mine: &Map<String, Value>, theirs: &Map<String, Value>, conflicts: &mut usize, depth: usize) -> Map<String, Value> {
    let names: BTreeSet<&String> = theirs.keys().chain(mine.keys()).chain(base.keys()).collect();
    let mut out = Map::new();
    for name in names {
        if let Some(v) = merge_inner(base.get(name), mine.get(name), theirs.get(name), conflicts, depth) {
            out.insert(name.clone(), v);
        }
    }
    out
}

/// The element's identity, when it is an object with a scalar `id`.
fn id_of(v: &Value) -> Option<String> {
    let Value::Object(o) = v else { return None };
    match o.get("id") {
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::String(s)) => Some(format!("s:{s}")),
        _ => None,
    }
}

/// The ids of every element, or `None` when one has none or two share one.
fn ids_of(list: &[Value]) -> Option<Vec<String>> {
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(list.len());
    for v in list {
        let id = id_of(v)?;
        if !seen.insert(id.clone()) {
            return None;
        }
        ids.push(id);
    }
    Some(ids)
}

fn by_id<'a>(ids: &'a [String], list: &'a [Value]) -> HashMap<&'a str, &'a Value> {
    ids.iter().map(String::as_str).zip(list.iter()).collect()
}

fn merge_id_arrays(base: &[Value], mine: &[Value], theirs: &[Value], conflicts: &mut usize, depth: usize) -> Option<Vec<Value>> {
    let (base_ids, mine_ids, their_ids) = (ids_of(base)?, ids_of(mine)?, ids_of(theirs)?);
    let (base_map, mine_map) = (by_id(&base_ids, base), by_id(&mine_ids, mine));
    let mut result: Vec<Value> = Vec::with_capacity(theirs.len().max(mine.len()));
    let mut result_ids: Vec<String> = Vec::with_capacity(result.capacity());
    let mut have: HashSet<String> = HashSet::new();
    // Their order is the base order of the result.
    for (id, their) in their_ids.iter().zip(theirs) {
        let kept = match (base_map.get(id.as_str()).copied(), mine_map.get(id.as_str()).copied()) {
            (Some(b), Some(m)) => merge_inner(Some(b), Some(m), Some(their), conflicts, depth),
            (Some(b), None) => {
                // I removed it. If they left it alone the removal stands; if they changed it, keep theirs.
                if b == their {
                    None
                } else {
                    *conflicts += 1;
                    Some(their.clone())
                }
            }
            (None, Some(m)) => {
                // We both added the same id.
                if m != their {
                    *conflicts += 1;
                }
                Some(their.clone())
            }
            (None, None) => Some(their.clone()),
        };
        if let Some(v) = kept {
            result.push(v);
            result_ids.push(id.clone());
            have.insert(id.clone());
        }
    }
    // Then what only I have: added by me, or changed by me while they removed it.
    for (index, (id, mine_value)) in mine_ids.iter().zip(mine).enumerate() {
        if have.contains(id) {
            continue;
        }
        match base_map.get(id.as_str()).copied() {
            // untouched by me, removed by them: stays removed
            Some(b) if b == mine_value => continue,
            // changed by me, removed by them: keep mine and say so
            Some(_) => *conflicts += 1,
            None => {}
        }
        let mut position = 0;
        for previous in mine_ids.iter().take(index).rev() {
            if let Some(p) = result_ids.iter().position(|x| x == previous) {
                position = p + 1;
                break;
            }
        }
        position = position.min(result.len());
        result.insert(position, mine_value.clone());
        result_ids.insert(position, id.clone());
        have.insert(id.clone());
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::merge3;

    fn m(base: Value, mine: Value, theirs: Value) -> (Value, usize) {
        let r = merge3(Some(&base), Some(&mine), Some(&theirs));
        (r.value.unwrap(), r.conflicts)
    }

    #[test]
    fn one_sided_changes_win_without_conflict() {
        assert_eq!(m(json!({"a": 1}), json!({"a": 2}), json!({"a": 1})), (json!({"a": 2}), 0));
        assert_eq!(m(json!({"a": 1}), json!({"a": 1}), json!({"a": 3})), (json!({"a": 3}), 0));
    }

    #[test]
    fn different_fields_both_survive() {
        let (v, c) = m(json!({"a": 1, "b": 1}), json!({"a": 2, "b": 1}), json!({"a": 1, "b": 9}));
        assert_eq!((v, c), (json!({"a": 2, "b": 9}), 0));
    }

    #[test]
    fn same_field_clash_takes_theirs_and_counts() {
        let (v, c) = m(json!({"a": 1}), json!({"a": 2}), json!({"a": 3}));
        assert_eq!((v, c), (json!({"a": 3}), 1));
    }

    #[test]
    fn clips_merge_by_id() {
        let base = json!({"items": [{"id": 1, "start": 0}, {"id": 2, "start": 10}]});
        let mine = json!({"items": [{"id": 1, "start": 5}, {"id": 2, "start": 10}]});
        let theirs = json!({"items": [{"id": 1, "start": 0}, {"id": 2, "start": 10}, {"id": 3, "start": 20}]});
        let (v, c) = m(base, mine, theirs);
        assert_eq!(c, 0);
        assert_eq!(v, json!({"items": [{"id": 1, "start": 5}, {"id": 2, "start": 10}, {"id": 3, "start": 20}]}));
    }

    #[test]
    fn my_added_clip_is_kept_after_their_removal_of_another() {
        let base = json!([{"id": 1}, {"id": 2}]);
        let mine = json!([{"id": 1}, {"id": 2}, {"id": 7}]);
        let theirs = json!([{"id": 2}]);
        let (v, c) = m(base, mine, theirs);
        assert_eq!((v, c), (json!([{"id": 2}, {"id": 7}]), 0));
    }

    #[test]
    fn removing_a_clip_the_other_side_changed_keeps_it_and_counts() {
        let base = json!([{"id": 1, "x": 0}]);
        let (v, c) = m(base.clone(), json!([]), json!([{"id": 1, "x": 4}]));
        assert_eq!((v, c), (json!([{"id": 1, "x": 4}]), 1));
        let (v, c) = m(base, json!([{"id": 1, "x": 4}]), json!([]));
        assert_eq!((v, c), (json!([{"id": 1, "x": 4}]), 1));
    }

    #[test]
    fn arrays_without_ids_are_leaves() {
        let (v, c) = m(json!({"k": [1, 2]}), json!({"k": [1, 3]}), json!({"k": [4]}));
        assert_eq!((v, c), (json!({"k": [4]}), 1));
    }

    #[test]
    fn duplicate_ids_fall_back_to_a_leaf() {
        let (v, c) = m(json!([{"id": 1}, {"id": 1}]), json!([{"id": 1, "a": 1}, {"id": 1}]), json!([{"id": 1}, {"id": 1, "b": 2}]));
        assert_eq!(c, 1);
        assert_eq!(v, json!([{"id": 1}, {"id": 1, "b": 2}]));
    }

    #[test]
    fn deleted_and_created_keys() {
        let r = merge3(Some(&json!({"a": 1})), None, Some(&json!({"a": 1})));
        assert_eq!(r.value, None);
        let r = merge3(None, Some(&json!({"a": 1})), Some(&json!({"a": 2})));
        assert_eq!((r.value, r.conflicts), (Some(json!({"a": 2})), 1));
    }

    #[test]
    fn deep_nesting_does_not_recurse_without_bound() {
        let mut deep = json!(1);
        for _ in 0..500 {
            deep = json!({"n": deep});
        }
        let mut a = deep.clone();
        let mut b = deep.clone();
        if let Some(o) = a.as_object_mut() {
            o.insert("x".into(), json!(1));
        }
        if let Some(o) = b.as_object_mut() {
            o.insert("y".into(), json!(2));
        }
        let r = merge3(Some(&deep), Some(&a), Some(&b));
        assert_eq!(r.conflicts, 0);
    }
}

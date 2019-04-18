use cpp::*;

cpp!{{
    #include "carmen-cache/src/memorycache.hpp"
}}

cpp_class!(pub unsafe struct MemoryCache as "carmen::MemoryCache");
impl MemoryCache {
    pub fn new() -> Self {
        unsafe { cpp!([] -> MemoryCache as "carmen::MemoryCache" { return carmen::MemoryCache(); }) }
    }

    pub fn pack(&self, filename: &str) -> bool {
        let filename_ptr = filename.as_ptr();
        let filename_len = filename.len();
        unsafe { cpp!([self as "carmen::MemoryCache*", filename_ptr as "const char*", filename_len as "size_t"] -> bool as "bool" {
            std::string filename(filename_ptr, filename_len);
            return self->pack(filename);
        }) }
    }

    // std::vector<std::pair<std::string, langfield_type>> list();
    //
    // void _set(std::string key_id, std::vector<uint64_t>, langfield_type langfield, bool append);
    //
    // std::vector<uint64_t> _get(std::string& phrase, std::vector<uint64_t> languages);
    // std::vector<uint64_t> _getmatching(std::string phrase, PrefixMatch match_prefixes, std::vector<uint64_t> languages);
    //
    // std::vector<uint64_t> __get(const std::string& phrase, langfield_type langfield);
    // std::vector<uint64_t> __getmatching(const std::string& phrase_ref, PrefixMatch match_prefixes, langfield_type langfield, size_t max_results);
}

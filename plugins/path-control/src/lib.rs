wit_bindgen::generate!({ path: "../../wit", world: "policy" });
struct PathControl;
impl Guest for PathControl {
    fn authorize(
        _method: String,
        path: String,
        _subject: String,
        _tenant: String,
        _operation: String,
        target: String,
    ) -> u16 {
        if path == "/admin" || path.starts_with("/admin/") || target == "admin_preview" {
            403
        } else {
            0
        }
    }
}
export!(PathControl);

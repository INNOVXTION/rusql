// outward API
trait DatabaseAPI {
    fn create_table(&self);
    fn drop_table(&self);

    fn insert(&self);
    fn select(&self);
    fn update(&self);
    fn delete(&self);
}

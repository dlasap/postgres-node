export default {
    filterableAttributes: ["tombstone", "id", "version", "status", "created_date", "update_date"],
    sortableAttributes: ["tombstone", "id", "version", "status", "created_date", "update_date"],
    //avoid searching values that are uuid - type
    searchableAttributes: ["tombstone", "version", "status", "created_date", "update_date"]
}
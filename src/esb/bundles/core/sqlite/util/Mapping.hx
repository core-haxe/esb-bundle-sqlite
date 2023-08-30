package esb.bundles.core.sqlite.util;

class Mapping {
    public function new() {
    }

    public function columnType(columnName:String) {
        return "string";
    }

    public function mappedColummName(columnName:String) {
        return columnName;
    }
}
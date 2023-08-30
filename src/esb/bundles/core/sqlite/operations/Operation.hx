package esb.bundles.core.sqlite.operations;

import promises.Promise;
import esb.core.bodies.RawBody;
import esb.core.Message;
import db.IDatabase;

class Operation {
    private var db:IDatabase;
    private var tableName:String;
    private var params:Map<String, Any>;

    public function new(db:IDatabase, tableName:String, params:Map<String, Any>) {
        this.db = db;
        this.tableName = tableName;
        this.params = params;
    }

    public function execute(message:Message<RawBody>):Promise<Message<RawBody>> {
        return new Promise((resolve, reject) -> {
            resolve(message);
        });
    }
}
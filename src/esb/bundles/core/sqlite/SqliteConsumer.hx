package esb.bundles.core.sqlite;

import esb.bundles.core.sqlite.operations.AddPivotOperation;
import esb.bundles.core.sqlite.operations.AddOperation;
import esb.core.bodies.RawBody;
import esb.core.Message;
import esb.bundles.core.sqlite.operations.QueryOperation;
import esb.bundles.core.sqlite.operations.Operation;
import db.DatabaseFactory;
import db.IDatabase;
import promises.Promise;
import esb.core.IBundle;
import esb.common.Uri;
import esb.core.IConsumer;
import esb.logging.Logger;
import esb.core.Bus.*;

using StringTools;

@:keep
class SqliteConsumer implements IConsumer {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.SqliteConsumer");

    public var bundle:IBundle;
    public function start(uri:Uri) {
        log.info('creating sqlite consumer for ${uri.toString()}');
        for (i in 0...1) {
            from(uri, (uri, message) -> {
                return new Promise((resolve, reject) -> {
                    var pathParts = uri.fullPath.split("/");
                    var operationName = pathParts.pop();
                    var tableName = pathParts.pop();
                    var dbName = pathParts.join("/");

dbName = "../test01.db";
                    var db:IDatabase = DatabaseFactory.instance.createDatabase(DatabaseFactory.SQLITE, {
                        filename: dbName
                    });

                    var operation:Operation = null;
                    switch (operationName) {
                        case "query":
                            operation = new QueryOperation(db, tableName, uri.params);
                        case "add":
                            var pivot:Bool = uri.paramBool("pivot", false);
                            if (pivot) {
                                operation = new AddPivotOperation(db, tableName, uri.params);
                            } else {
                                operation = new AddOperation(db, tableName, uri.params);
                            }
                        case _:
                            log.warn('operation "${operationName}" not found');
                    }

                    var resultMessage:Message<RawBody> = null;
                    if (operation == null) {
                        resolve(message);
                    } else {
                        db.connect().then(result -> {
                            return operation.execute(message);
                        }).then(result -> {
                            resultMessage = result;
                            return db.disconnect();
                        }).then(result -> {
                            resolve(resultMessage);
                        }, error -> {
                            trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR!", error);
                            db.disconnect().then(_ -> {
                                resolve(message);
                            });
                        });
                    }
                });
            });
        }
    }
}
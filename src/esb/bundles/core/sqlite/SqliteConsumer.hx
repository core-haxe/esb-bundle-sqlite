package esb.bundles.core.sqlite;

import db.Record;
import db.DatabaseFactory;
import db.IDatabase;
import esb.core.bodies.CsvBody;
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
                    // TODO: do better
                    var pathParts = uri.fullPath.split("/");
                    var tableName = pathParts.pop();
                    var dbName = pathParts.join("/");

                    var db:IDatabase = DatabaseFactory.instance.createDatabase(DatabaseFactory.SQLITE, {
                        filename: dbName
                    });

                    if (canConvertMessage(message, CsvBody)) {
                        var csvMessage = convertMessage(message, CsvBody);
                        var fields = uri.param("fields");
                        var pivot:Bool = uri.paramBool("pivot", false);
                        if (fields != null) {
                            var records = [];
                            for (row in csvMessage.body.data) {
                                var record = new Record();
                                for (field in fields.split(",")) {
                                    field = field.trim();
                                    if (field.length == 0) {
                                        continue;
                                    }

                                    var fieldIndex = csvMessage.body.columns.indexOf(field);
                                    if (fieldIndex == -1) {
                                        continue;
                                    }

                                    
                                    var value = row[fieldIndex];
                                    record.field(field, value);
                                }
                                records.push(record);
                            }

                            db.connect().then(result -> {
                                return result.database.table(tableName);
                            }).then(result -> {
                                return result.table.addAll(records);
                            }).then(result -> {
                                return db.disconnect();
                            }).then(result -> {
                                resolve(message);
                            }, error -> {
                                trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR!", error);
                                db.disconnect().then(_ -> {
                                    resolve(message);
                                });
                            });
                        } else if (pivot) {
                            var records = [];
                            for (row in csvMessage.body.data) {
                                var pk = null;
                                var pkName = uri.param("primaryKey");
                                var ignoreColumns = uri.param("ignoreFields", "");
                                for (n in 0...csvMessage.body.columns.length) {
                                    var column = csvMessage.body.columns[n];
                                    var value = row[n];
                                    if (column == pkName) {
                                        pk = value;
                                        break;
                                    }
                                }

                                for (n in 0...csvMessage.body.columns.length) {
                                    var column = csvMessage.body.columns[n];
                                    if (column == pkName) {
                                        continue;
                                    }
                                    if (ignoreColumns.contains(column)) {
                                        continue;
                                    }
                                    var value = row[n];

                                    var record = new Record();
                                    record.field(pkName, pk);
                                    record.field("FieldName", column);
                                    record.field("FieldValue", value);
                                    record.field("FieldType", "unknown");
                                    records.push(record);
                                }
                            }
                            db.connect().then(result -> {
                                return result.database.table(tableName);
                            }).then(result -> {
                                return result.table.addAll(records);
                            }).then(result -> {
                                return db.disconnect();
                            }).then(result -> {
                                resolve(message);
                            }, error -> {
                                trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR!", error);
                                db.disconnect().then(_ -> {
                                    resolve(message);
                                });
                            });
                        } else {
                            db.disconnect().then(_ -> {
                                resolve(message);
                            });
                        }

                        /*
                        trace(csvMessage.body.columns, csvMessage.body.data);
                        for (row in csvMessage.body.data) {
                            for (n in 0...csvMessage.body.columns.length) {
                                var column = csvMessage.body.columns[n];
                                var value = row[n];
                                trace(">>>>>>>>>>>>>>>>>>> " + column + " = " + value);
                            }
                        }
                        */
                    } else {
                        db.disconnect().then(_ -> {
                            resolve(message);
                        });
                    }
                });
            });
        }
    }
}
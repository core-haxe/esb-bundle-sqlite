package esb.bundles.core.sqlite.operations;

import esb.bundles.core.sqlite.util.Mapping;
import db.Record;
import esb.core.bodies.CsvBody;
import promises.Promise;
import esb.core.bodies.RawBody;
import esb.core.Message;
import esb.core.Bus.*;
import esb.logging.Logger;

using StringTools;

class AddPivotOperation extends Operation {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.operations.AddOperation");

    public override function execute(message:Message<RawBody>):Promise<Message<RawBody>> {
        return new Promise((resolve, reject) -> {
            log.info('adding pivot data to "${tableName}"');

            if (canConvertMessage(message, CsvBody)) {
                var csvMessage = convertMessage(message, CsvBody);

                var records = [];
                var pkName = "" + params.get("primaryKey");
                var ignoreColumns = [];
                if (params.get("ignoreColumns") != null) {
                    var columnList = "" + params.get("ignoreColumns");
                    ignoreColumns = columnList.split(",").map(item -> item.trim());
                }

                var mapping = new Mapping();
                for (row in csvMessage.body.data) {
                    var pk = null;
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

                        var mappedColumn = mapping.mappedColummName(column);
                        var value = row[n];
                        var record = new Record();
                        record.field(pkName, pk);
                        record.field(mapping.mappedColummName("fieldName"), mappedColumn);
                        record.field(mapping.mappedColummName("fieldValue"), value);
                        record.field(mapping.mappedColummName("fieldType"), mapping.columnType(mappedColumn));
                        records.push(record);
                    }
                }

                db.table(tableName).then(result -> {
                    return result.table.addAll(records);
                }).then(result -> {
                    resolve(message);
                }, error -> {
                    reject(error);
                });
            } else {
                trace(">>>>>>>>>>>>>>>>>>>>>>>>>> WE CANT CONVERT!");
                resolve(message);
            }
        });
    }
}
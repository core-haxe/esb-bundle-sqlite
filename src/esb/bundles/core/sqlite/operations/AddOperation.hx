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

class AddOperation extends Operation {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.operations.AddOperation");

    public override function execute(message:Message<RawBody>):Promise<Message<RawBody>> {
        return new Promise((resolve, reject) -> {
            log.info('adding data to "${tableName}"');

            if (canConvertMessage(message, CsvBody)) {
                var csvMessage = convertMessage(message, CsvBody);
                var columns:Array<String> = csvMessage.body.columns;
                if (params.get("columns") != null) {
                    var columnList = "" + params.get("columns");
                    columns = columnList.split(",").map(item -> item.trim());
                }
                log.info('column list: ${columns}');

                var records = [];
                var mapping = new Mapping();
                for (row in csvMessage.body.data) {
                    var record = new Record();
                    for (column in columns) {
                        var columnIndex = csvMessage.body.columns.indexOf(column);
                        if (columnIndex == -1) {
                            continue;
                        }

                        var value = row[columnIndex];
                        record.field(mapping.mappedColummName(column), value);
                    }
                    records.push(record);
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
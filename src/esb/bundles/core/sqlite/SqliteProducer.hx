package esb.bundles.core.sqlite;

import esb.core.config.sections.EsbConfig;
import esb.core.bodies.CsvBody;
import db.ITable;
import promises.Promise;
import db.Record;
import db.DatabaseFactory;
import db.IDatabase;
import esb.bundles.core.sqlite.util.Mapping;
import esb.core.IBundle;
import esb.common.Uri;
import esb.core.IProducer;
import esb.logging.Logger;
import Query.*;
import Query.QueryExpr;
import esb.core.Bus.*;

using StringTools;

@:keep
class SqliteProducer implements IProducer {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.SqliteProducer");

    public var bundle:IBundle;
    public function start(uri:Uri) {
        log.info('creating sqlite producer for ${uri.toString()}');

        var pathParts = uri.fullPath.split("/");
        var tableName = pathParts.pop();
        var dbName = pathParts.join("/");

        dbName = EsbConfig.get().path(dbName, false);

        log.info('waiting for new records in "${dbName}"."${tableName}"');
        lookForPendingRecords(uri);
    }

    private function lookForPendingRecords(uri:Uri) {
        var pollInterval = uri.paramInt("pollInterval", 1000);
        var pkName = uri.param("primaryKey");
        if (pkName == null) {
            trace(">>>>>>>>>>>>>>>>>>>>>>>>>> NO PRIMARY KEY");
            haxe.Timer.delay(lookForPendingRecords.bind(uri), pollInterval);
            return;
        }

        var pathParts = uri.fullPath.split("/");
        var tableName = pathParts.pop();
        var dbName = pathParts.join("/");

        dbName = EsbConfig.get().path(dbName, false);

        var mapping = new Mapping();
        var statusColumn = mapping.mappedColummName(uri.param("statusColumn", "Status"));
        var pendingValue = uri.param("pendingValue", "pending");
        var processingValue = uri.param("processingValue", "processing");
        var completeValue = uri.param("completeValue", "complete");

        var query = QueryBinop(QOpAssign, QueryConstant(QIdent(statusColumn)), QueryConstant(QString(pendingValue)));

        var db:IDatabase = DatabaseFactory.instance.createDatabase(DatabaseFactory.SQLITE, {
            filename: dbName
        });

        db.connect().then(result -> {
            return db.table(tableName);
        }).then(result -> {
            return result.table.findOne(query, false);
        }).then(result -> {
            if (result.data != null && result.data.fieldNames.length > 0) {
                pollInterval = 0;
                return processRecord(uri, result.data, result.table);
            }
            return null;
        }).then(result -> {
            return db.disconnect();
        }).then(result -> {
            haxe.Timer.delay(lookForPendingRecords.bind(uri), pollInterval);
            return null;
        }, error -> {
            trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR!", error);
            db.disconnect().then(_ -> {
                haxe.Timer.delay(lookForPendingRecords.bind(uri), pollInterval);
            });
        });

    }

    private function processRecord(uri:Uri, record:Record, table:ITable):Promise<Bool> {
        return new Promise((resolve, reject) -> {
            var start = Sys.time();
            var mapping = new Mapping();
            var statusColumn = mapping.mappedColummName(uri.param("statusColumn", "Status"));
            var processingValue = uri.param("processingValue", "processing");
            var completeValue = uri.param("completeValue", "complete");

            var pkName = uri.param("primaryKey");
            var pkValue = record.field(pkName);
            record.field(statusColumn, processingValue);
            var query = QueryBinop(QOpAssign, QueryConstant(QIdent(pkName)), QueryConstant(QString(pkValue)));
            table.update(query, record).then(result -> {
                var message = createMessage(CsvBody);
                message.body.columns = record.fieldNames;
                message.body.data = [record.values()];
                to(uri, cast message).then(resultMessage -> {
                    record.field(statusColumn, completeValue);
                    table.update(query, record).then(result -> {
                        var end = Sys.time();
                        trace("-------------------------------------------------> ALL DONE IN: ", Math.round((end - start) * 1000) + " ms");
                        resolve(true);
                    });
                });
            }, error -> {
                reject(error);
            });
        });
    }
}
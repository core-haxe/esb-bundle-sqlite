package esb.bundles.core.sqlite.operations;

import esb.core.bodies.CsvBody;
import promises.Promise;
import esb.core.bodies.RawBody;
import esb.core.Message;
import Query.*;
import Query.QueryExpr;
import esb.core.Bus.*;
import esb.logging.Logger;

class QueryOperation extends Operation {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.operations.QueryOperation");

    public override function execute(message:Message<RawBody>):Promise<Message<RawBody>> {
        return new Promise((resolve, reject) -> {
            var parts = [];
            for (paramName in params.keys()) {
                var paramValue = params.get(paramName);
                var qp = QueryBinop(QOpAssign, QueryConstant(QIdent(paramName)), QueryConstant(QString(paramValue)));
                parts.push(qp);
            }
            var query:QueryExpr = null;
            if (parts.length > 1) {
                var last = parts.pop();
                var beforeLast = parts.pop();
                var qp = QueryBinop(QOpBoolAnd, beforeLast, last);
                while (parts.length > 0) {
                    var q = parts.pop();
                    qp = QueryBinop(QOpBoolAnd, q, qp);
                }
                query = qp;
            } else {
                query = parts[0];
            }

            log.info('performing query on "${tableName}" (${queryExprToSql(query)})');

            db.table(tableName).then(result -> {
                return result.table.find(query, false);
            }).then(result -> {
                var csvMessage = copyMessage(message, CsvBody);
                csvMessage.body.columns = [];
                csvMessage.body.data = [];
                if (result.data.length > 0) {
                    var firstRecord = result.data[0];
                    csvMessage.body.addColumns(firstRecord.fieldNames);
                }

                log.info("query resulted in " + result.data.length + " row(s)");

                for (record in result.data) {
                    csvMessage.body.addRow(record.values());
                }
                resolve(cast csvMessage);
            }, error -> {
                reject(error);
            });

        });
    }
}
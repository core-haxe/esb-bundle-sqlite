package esb.bundles.core.sqlite;

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
        from(uri, message -> {
            return new Promise((resolve, reject) -> {
            });
        });
    }
}
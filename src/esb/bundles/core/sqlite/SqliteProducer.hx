package esb.bundles.core.sqlite;

import esb.core.IBundle;
import esb.common.Uri;
import esb.core.IProducer;
import esb.logging.Logger;

using StringTools;

@:keep
class SqliteProducer implements IProducer {
    private static var log:Logger = new Logger("esb.bundles.core.sqlite.SqliteProducer");

    public var bundle:IBundle;
    public function start(uri:Uri) {
        log.info('creating sqlite producer for ${uri.toString()}');
    }
}
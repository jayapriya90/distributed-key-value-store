include "shared.thrift"

namespace java edu.umn.cs.distributedkeyvaluestore

service FileServerEndPoints {
     // called by clients
     shared.ReadResponse read(1:string filename);
     shared.WriteResponse write(1:string filename, 2:string contents);
     shared.FileServerMetaData getFileServerMetadata();

     // called by coordinator
     shared.ReadResponse readContents(1:string filename);
     shared.WriteResponse writeContents(1:string filename, 2:string contents);
     shared.WriteResponse updateContentsToVersion(1:string filename, 2:string contents, 3:i64 version);
     i64 getVersion(1:string filename);
}

include "shared.thrift"

namespace java edu.umn.cs.distributedkeyvaluestore

struct FileServerResponse {
    1: required shared.Status status;
    2: optional shared.FileServerInfo fileServerInfo;
}

enum Type {
    READ,
    WRITE
}

struct Request {
    1: required Type type;
    2: required string filename;
    3: optional string contents; // this will be set only if Type is WRITE
}

struct Response {
    1: required Type type;
    2: optional shared.ReadResponse readResponse;
    3: optional shared.WriteResponse writeResponse;
}

service CoordinatorEndPoints {
     // Used by client
     FileServerResponse getFileServer(),
     map<shared.FileServerInfo, shared.FileServerMetaData> getMetadata(),

     // Used by fileservers
     void join(1:string hostname, 2:i32 Port),
     Response submitRequest(1: Request request),
}

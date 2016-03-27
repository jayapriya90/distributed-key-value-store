namespace java edu.umn.cs.distributedkeyvaluestore

struct FileServerInfo {
  1: required string hostname,
  2: required i32 Port;
}

struct FileServerMetaData {
   1: list<FileInfo> fileinfos;
}

struct FileInfo {
   1: required string filename,
   2: required i64 version,
   3: required string contents;
}

enum Status {
    FILE_NOT_FOUND,
    SUCCESS,
    //NO_NODE_FOUND
}
struct ReadResponse {
    1: required Status status;
    2: optional string contents;
    3: optional i64 version;
}

struct WriteResponse {
    1: required Status status;
    2: optional i32 bytesWritten;
    3: optional i64 version;
}






# pytutor-fs-exploration

- File system tree data comes in as an SSE feed from backend, of the shape: 

```ts
interface FileMetaFields {
  id: string;
  name: string;
  parentId: string | null; // null = workspace root
  type: "file" | "folder";
  deleted: boolean;
  version: string;
}
```

- On an initial load, this whole shape will come in as a response to fetching the wokspace

NOTE On Internet Connectivity: Though we want to be resilient to loss of internet connection while using the app, we assume that if the user doesn't have internet, they won't be able to load the workspace in the first place. So it's guarranteed that the initial fs is seeded with the most up to date information the server has.

Client->Backend
- create entry
  - on success response: Associate 
  - on "can't complete transaction" response: shouldn't fail / failure isn't recoverable
  - on error: 
- delete entry:
  - on success: 
- change name
- change parent
- write content (includes from->to

Constraints: 
- A file needs a succesful entry in the metadata map to be opened, and a folder needs a succesful entry in metadata map to have a file added to it. Therefore, new files created without an internet connection won't be able to be opened, and new folders created without an internet connection won't be able to have any entries added to it. 


```ts
namespace FileSystem  {
  export namespace Entry {
    export type Type = "file" | "folder";

    export type Metadata = {
        id: string;
        name: string;
        parentId: string | null; // null = workspace root
        type: "file" | "folder";
        deleted: boolean;
        version: string;
    };
  }

  export namespace Events {
    export namespace ClientSent {
        export namespace namespace Create {
          export type Request {
            type: FileSystem.Entry.Type;
            parent: Pick<FileSystem.Entry.Metadata, "id" | "version"> | null; // null = workspace root
          }
        
          export namespace Response {
            export type Success = FileSystem.Entry.Metadata;
        
            export type Failure {
              reason: "Parent was deleted. Cannot add entry to a deleted parent"
            }
          }
        }

        export namespace Delete {
          
        }
      }
    }
  }
}
```

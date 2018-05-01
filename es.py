from elasticsearch import Elasticsearch
import json

class ES():   
    '''TODO: Rearange and better encapsulate this class.
       This is a little messy!'''

    def __init__(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if not self.es.exists(index="ff", doc_type="files_folders", id=31):
            self.populate()
        self.output = {}
        self.folders = []


    def populate(self):
        # populate "ff" index with folders and files
        # beside keys and values from ffs list adds "id" and
        # path to root folder as "DS_Parent_Tree"
        ffs = [
                {"DS_Name": "folder1", "DS_Type": "dir", "DS_Parent": "null"},
                {"DS_Name": "folder2", "DS_Type": "dir", "DS_Parent": "1"},
                {"DS_Name": "folder3", "DS_Type": "dir", "DS_Parent": "2"},
                {"DS_Name": "aBc.txt", "DS_Type": "file", "DS_Parent": "3"},
                {"DS_Name": "ABb.txt", "DS_Type": "file", "DS_Parent": "3"},
                {"DS_Name": "xyz.txt", "DS_Type": "file", "DS_Parent": "3"},
                {"DS_Name": "folder4", "DS_Type": "dir", "DS_Parent": "2"},
                {"DS_Name": "Folder10", "DS_Type": "dir", "DS_Parent": "7"},
                {"DS_Name": "oaxbco.pdf", "DS_Type": "file", "DS_Parent": "8"},
                {"DS_Name": "ddAb.doc", "DS_Type": "file", "DS_Parent": "8"},
                {"DS_Name": "folder111", "DS_Type": "dir", "DS_Parent": "8"},
                {"DS_Name": "file.pdf", "DS_Type": "file", "DS_Parent": "11"},
                {"DS_Name": "fileab.zip", "DS_Type": "file", "DS_Parent": "11"},
                {"DS_Name": "somefile.doc", "DS_Type": "file", "DS_Parent": "11"},
                {"DS_Name": "fo.txt", "DS_Type": "file", "DS_Parent": "11"},
                {"DS_Name": "Kkk.zip", "DS_Type": "file", "DS_Parent": "2"},
                {"DS_Name": "Folder19", "DS_Type": "dir", "DS_Parent": "2"},
                {"DS_Name": "Dd.doc", "DS_Type": "file", "DS_Parent": "17"},
                {"DS_Name": "Oacb.pdf", "DS_Type": "file", "DS_Parent": "17"},
                {"DS_Name": "Folder32", "DS_Type": "dir", "DS_Parent": "17"},
                {"DS_Name": "Kab.pdf", "DS_Type": "file", "DS_Parent": "20"},
                {"DS_Name": "ygabe.docx", "DS_Type": "file", "DS_Parent": "20"},
                {"DS_Name": "kabcd.xls", "DS_Type": "file", "DS_Parent": "1"},
                {"DS_Name": "folder5", "DS_Type": "dir", "DS_Parent": "1"},
                {"DS_Name": "folder6", "DS_Type": "dir", "DS_Parent": "24"},
                {"DS_Name": "folder88", "DS_Type": "dir", "DS_Parent": "25"},
                {"DS_Name": "test.doc", "DS_Type": "file", "DS_Parent": "26"},
                {"DS_Name": "fo.docs", "DS_Type": "file", "DS_Parent": "25"},
                {"DS_Name": "xaby.txt", "DS_Type": "file", "DS_Parent": "25"},
                {"DS_Name": "lkABdocx", "DS_Type": "file", "DS_Parent": "25"},
                {"DS_Name": "qqa.docx", "DS_Type": "file", "DS_Parent": "24"}]
        i = 1
        for f in ffs:
            if f["DS_Parent"] == "null":
                f["DS_Parent_Tree"] = []
            else:
                lst = ffs[int(f["DS_Parent"]) - 1]["DS_Parent_Tree"][:]
                lst.append(f["DS_Parent"])
                f["DS_Parent_Tree"] = lst
            self.es.index(index='ff', doc_type='files_folders', id=i, body=f)
            i += 1
        self.es.indices.put_settings(index="ff",
                        body= {"index" : {
                                "max_result_window" : 31
                              }})
    

    def return_output(self, dct):
        # arguments: result of a search query as dict
        # returns: list of root folders with filtered data and populated "children" list
        # example: {"DS_Name": "folder1", "DS_Type": "dir", "DS_Parent": "null", children[ {"DS_Name": "folder2"...}]}
        
        # filters data from search result and adds them to output
        # id's of folders from "DS_Parent_Tree" added to folders list
        for e in dct["hits"]["hits"]:
            #print(e["_id"])
            self.output[e["_id"]] = {"_id": e["_id"], "DS_Name": e["_source"]["DS_Name"],
                                     "DS_Type": e["_source"]["DS_Type"],
                                     "DS_Parent": e["_source"]["DS_Parent"], "children": []}
            self.folders += e["_source"]["DS_Parent_Tree"]
        # adds parent folders to output dict
        for e in set(self.folders):
            folder = self.es.get(index="ff", doc_type="files_folders", id=e)
            self.output[e] = {"_id": folder["_id"], "DS_Name": folder["_source"]["DS_Name"],
                              "DS_Type": folder["_source"]["DS_Type"],
                              "DS_Parent": folder["_source"]["DS_Parent"], "children": []}
        # populate children list in output dicts
        for value in self.output.values():
            try:
                par = value["DS_Parent"]
                self.output[par]["children"].append(value)
            except KeyError:
                continue
        # returns only a root folders with "DS_Parent" value "null"
        return [value for value in self.output.values() if value["DS_Parent"] == "null"]


    def query_all(self, query):
        # search for files and folders which match query and
        # returns json with result and folders from their path
        query_all = {
                "size" : 31,
                "query": {
                    "query_string": {
                        "default_field" : "DS_Name",
                        "query":"*{}*".format(query)
                    }
                }
            }
        dct = self.es.search(index="ff", body=query_all)
        return json.dumps(self.return_output(dct), indent=4)
    

    def query_files(self, query):
        # search only for files which match query and
        # returns json with result and folders from their path
        query_files = {
                "size" : 31,
                "query": {
                    "bool": {
                        "must": {
                            "query_string": {
                                "default_field" : "DS_Name",
                                "query": "*{}*".format(query)
                            }
                        },
                        "filter": {
                            "term": {
                                "DS_Type": "file"
                            }
                        }
                    }
                }
            }
        dct = self.es.search(index="ff", body=query_files)
        return json.dumps(self.return_output(dct), indent=4)


#print(es.get(index='ff', doc_type='files_folders', id=1))
#es.indices.delete(index='ff', ignore=[400, 404])
#es = ES()
#print(es.query_files("fo"))
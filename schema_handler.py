import json as js
import os
from datetime import datetime as dt 
archive_date = str(dt.now().strftime('%Y_%m_%d'))

class Schema_Handler:
    dir_path = ""
    arc_path = ""
    schema_file = ""
    schema_version = ""
    new_schema_file = ""
    logger = ""

    def __init__(self, schema_file, dir_path, arc_path ):
        self.dir_path = dir_path
        self.arc_path = arc_path
        self.schema_file = schema_file
        self.schema_version = int(schema_file.split('_v')[1].split('.')[0])
        self.new_schema_file = schema_file.replace('_v'+str(self.schema_version), '_v'+str(self.schema_version+1))
        #self.logger = logger

    def get_schema(self):
        # read json schema file and return dictionary
        with open(self.dir_path+'/'+self.schema_file, mode='r', encoding='utf-8') as fptr:
            json_schema = js.load(fptr)
            return json_schema


    def update_schema(self, new_fields):
        """
        :param new_fields: identified new fields will be updated on current schema json
        :return: json schema
        """        
        # converting incoming string into dictionary object 
        json_schema = js.loads(self.get_schema())
                
        # adding new fields into input schema json
        for key, val in new_fields.items():
            element = {'metadata':{}, 'name': key, 'nullable': True, 'type': val}            
            json_schema['fields'].append(element)
            
        # move current input schema file to archive
        os.rename(self.dir_path+'/'+self.schema_file , self.arc_path+'/'+archive_date+'_'+self.schema_file)
        
        # converting string json into JSON object
        json_schema = js.dumps(json_schema)            
        
        # save updated schema to new file 
        with open(self.dir_path+'/'+self.new_schema_file,'w', encoding='utf-8') as fptr:
            js.dump(json_schema, fptr, indent=4)
        
        return json_schema
        

        
    def schema_validate(self, source_df):
        """
        :schema_validate () : Method to validate user input and source schema 
        :param : dataframe        
        :return type : JSON schema 
        """
        
        result_dict = {}
        
        # check user input schema file is empty or not
        if os.stat(self.dir_path+'/'+self.schema_file).st_size == 0:
            # if user input schema file is empty then get source schema 
            source_schema_json = source_df.schema.json()
                                    
            # move current file to archive
            os.rename(self.dir_path+'/'+self.schema_file , self.arc_path+'/'+archive_date+'_'+self.schema_file)
            
            # save source schema to new file             
            with open(self.dir_path+'/'+self.new_schema_file,'w', encoding='utf-8') as fptr:
                js.dump(source_schema_json, fptr, indent=4)
            
            result_dict['schema']= source_schema_json
            result_dict['new_fields'] = None
            result_dict['missing_fields'] = None
            
        else:
            # if user input schema file is not empty then compare source and input schema jsons
            input_schema_json = js.loads(self.get_schema())
            
            # get source json for comparison
            source_schema_json = js.loads(source_df.schema.json())
                        
            
            # getting field and type in dictionary from source 
            source_fields_dict = { src_dict['name']:src_dict['type'] 
                                  for src_dict in source_schema_json['fields'] }
            
            # getting field and type in dictionary from user input schema
            input_fields_dict = { inp_dict['name']:inp_dict['type'] 
                                 for inp_dict in input_schema_json['fields'] }
            
            
            # identify new fields from source schema which are not in user input schema 
            new_fields = { key: val for (key, val) in source_fields_dict.items() 
                          if (key, val) not in input_fields_dict.items()}
            
            
            # check for missing fields from source schema which are prsent in input schema                 
            missing_fields = { key: val for (key, val) in input_fields_dict.items() 
                          if (key, val) not in source_fields_dict.items() }
            
            
            # setting result dictionary as default schema
            result_dict['schema'] = input_schema_json
            result_dict['new_fields'] = None
            result_dict['missing_fields'] = None
                        
            
            # checking for euquality # if TRUE then no schema change 
            if len(source_fields_dict) == len(input_fields_dict) and len(new_fields) == 0 and len(missing_fields) ==0 : 
                print('both schmea are same')
                result_dict['schema'] = input_schema_json
                result_dict['new_fields'] = None
                result_dict['missing_fields'] = None
        
            else:                                             
                # check for existance of new fields
                if len(new_fields) != 0:                    
                    # if new fields found then update schema
                    updated_json_schmea = self.update_schema(new_fields)
                    result_dict['schema'] = updated_json_schmea
                    result_dict['new_fields'] = new_fields
                            
                # identify missing fields
                if len(missing_fields) != 0:                    
                    # add missing fields into return dictionary                    
                    result_dict['missing_fields'] = missing_fields
                                        
                 
        return result_dict
                    
if __name__ == "__main__":
    print(__name__)


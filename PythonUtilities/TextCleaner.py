import gc
import pickle
import pandas as pd
import text_cleaning_functions as cf

'''
  By Brian Laich Data Engineer and ML / AI Engineer
  This class is used to clean csv data files from one formatting to another and output the results as fields of strings 
  It utilizes the following classes 
    gc -> garbage collector for python 
    pickle -> object dumping if needed 
    pandas -> numerical python library for data manipulation 
    cleaning_functions -> various cleaning functions set up as standalone functions 
  It utilizes the following parameters 
    full_input_path_to_text_to_clean -> the path to the input text file to clean. You need access, and it should be local to the space doing the calling. 
    full_output_path_to_clean_text -> the path to write the output text file to. You need access, and it should be local to the space doing the calling.
    input_file_delimiter -> current delimiter used in the input file 
    output_file_delimiter -> output delimiter to use in the output file
    Pandas specific arguments (see more at https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
      index_col, header, quote_char (alias for quotechar), escape_char (alias for escapechar), on_bad_lines, 
    String map arguments -> These map substring components in fields to new values 
      empty_string_map -> map empty strings to a new value
      not_null_map -> map strings normally recorded as null to a new value 
      additional_field_map -> map additional substrings to a new value 
    remove_metatags -> boolean on whether or not to attempt to remove HTML like tags <> 
    metatag_replacement -> what to replace those with 
    spacing_between_items_in_fields -> how much spacing to reset the mutated no line break format to between items in fields 
    string_notation_char -> what each field should start and end with 
    Encoding types 
      Types are needed for input and output files. Defaults to UTF-8
'''
class text_cleaner:
  def __init__(self, full_input_path_to_text_to_clean, full_output_path_to_clean_text,
              input_file_delimiter=',', output_file_delimiter=',', header=None, 
              index_col=False, na_values=[], keep_default_na=True, 
              quote_char='"', escape_char='\\', on_bad_lines='warn',   
              empty_string_map={}, not_null_map={}, 
              additional_field_map={}, 
              remove_metatags=True, metatag_replacement='',
              spacing_between_items_in_fields=1, 
              string_notation_char = '"',
              input_encoding_type='utf-8', output_encoding_type='utf-8') : 
    self.full_input_path_to_text_to_clean = full_input_path_to_text_to_clean
    self.full_output_path_to_clean_text = full_output_path_to_clean_text
    self.input_file_delimiter = input_file_delimiter
    self.output_file_delimiter = output_file_delimiter
    self.header = header
    self.index_col = index_col
    self.na_values = na_values
    self.keep_default_na = keep_default_na
    self.quote_char = quote_char
    self.escape_char = escape_char
    self.on_bad_lines = on_bad_lines 
    self.empty_string_map = empty_string_map
    self.not_null_map = not_null_map
    self.additional_field_map = additional_field_map
    self.remove_metatags = remove_metatags
    self.metatag_replacement= metatag_replacement
    self.spacing_between_items_in_fields = spacing_between_items_in_fields
    self.input_encoding_type = input_encoding_type 
    self.output_encoding_type = output_encoding_type
    self.input_file_loaded = False 
    self.input_file_loaded_as_dataframe = False 
    self.input_file_as_dataframe = None 
    self.output_file_written = False 
    self.input_file = None
    self.string_notation_char = string_notation_char

  '''
    Attempt to load text file. 
    If opened, will be opened in read only mode. 
  '''
  def attempt_load_text(self):
    try : 
      self.input_file = open(self.full_input_path_to_text_to_clean, 'r')
      self.input_file_loaded = True
    except : 
      raise Exception(f"Error opening file at {self.full_input_path_to_text_to_clean}. Address before reattempting loading")

  '''
    Update full input path to text to clean. Useful if handling several files 
  '''
  def update_full_input_path_to_text_to_clean(self, new_full_input_path_to_text_to_clean) : 
    self.full_input_path_to_text_to_clean = new_full_input_path_to_text_to_clean 
    if self.input_file_loaded : 
      self.input_file_loaded = False 

  '''
    As above for output 
  '''
  def update_full_output_path_to_clean_text(self, new_full_output_path_to_clean_text) : 
    self.full_output_path_to_clean_text = new_full_output_path_to_clean_text 
    if self.output_file_written : 
      self.output_file_written = False

  '''
    Releases loaded dataframe 
  '''
  def release_loaded_dataframe(self) : 
    if self.input_file_as_dataframe is not None : 
      current_loaded_dataframe = self.input_file_as_dataframe
      del current_loaded_dataframe
      self.input_file_as_dataframe = None 
      gc.collect() 
    return 

  '''
    Attempt to load text to dataframe (does a read processing using arguments provided)
  '''
  def attempt_load_text_to_dataframe(self) : 
    if self.input_file_as_dataframe is not None : 
      self.release_loaded_dataframe()
    try : 
      self.input_file_as_dataframe = pd.read_csv(self.full_input_path_to_text_to_clean, 
                                                 sep=self.input_file_delimiter,
                                                 header=self.header,
                                                 index_col=self.index_col,
                                                 na_values=self.na_values,
                                                 keep_default_na=self.keep_default_na,
                                                 quotechar=self.quote_char,
                                                 escapechar=self.escape_char,
                                                 on_bad_lines=self.on_bad_lines)
      self.input_file_loaded_as_dataframe = True 
    except : 
      raise Exception(f"Error loading {self.full_input_path_to_text_to_clean} as a dataframe")

  '''
    Cleans dataframe to a text file output (can be specified as csv, or other file extensions writable from character streams)
  '''
  def clean_dataframe_to_text_file(self) : 
    '''
      If the input file is not loaded, attempt to load it and then proceed to process it 
    '''
    try : 
      if self.input_file_as_dataframe is None : 
        self.attempt_load_text_to_dataframe()
      if self.input_file_as_dataframe is not None : 
        ''' By building output file strings '''
        output_file_strings = [] 
        rows, cols = self.input_file_as_dataframe.shape
        '''By looping over rows iteratively'''
        for _index, row in self.input_file_as_dataframe.iterrows() :
          '''Picking out the columns'''
          row_fields = [] 
          for col in range(cols) : 
            string_form = str(row[col])
            '''Dealing with empty strings'''
            if len(string_form) == 0 : 
              row_fields.append(cf.replace_empty_string_with_target_string(string_form, self.empty_string_map.get(str(string_form), '""')))
            else : 
              '''Dealing with not null strings, additional fields, and metatags'''
              for not_null_field, replacement in self.not_null_map.items() : 
                string_form = cf.replace_source_string_with_target_string(string_form, not_null_field, replacement)
              for additional_field, replacement in self.additional_field_map.items() :
                string_form = cf.replace_source_string_with_target_string(string_form, additional_field, replacement)
              if self.remove_metatags is True : 
                string_form = cf.remove_metatags_from_string(string_form, self.metatag_replacement)
              '''Mutation to achieve multiple lines as a single line and to get set spaces to be the amount desired'''
              string_form = cf.mutate_multi_lines_and_spacings_to_single_line_set_spaces(string_form, self.spacing_between_items_in_fields)
              '''Ensuring string formatting consistently'''
              string_form = cf.prepend_string_if_missing(string_form, self.string_notation_char)
              string_form = cf.append_string_if_missing(string_form, self.string_notation_char)
              row_fields.append(string_form)
          '''Join up into a single file line'''
          output_file_strings.append(self.output_file_delimiter.join(row_fields))
        try : 
          '''Try to write to the appropriate spot. If it fails, we dump the file as a pickle so you can go find out what is wrong'''
          output_file = open(self.full_output_path_to_clean_text, 'w', encoding=self.output_encoding_type)
          output_file.write('\n'.join(output_file_strings))
          output_file.close()
          self.output_file_written = True
        except : 
          # pickle the results just in case 
          dumpfile = open(str(self.full_output_path_to_clean_text) + '.pickle', 'wb')
          pickle.dump(output_file_strings, dumpfile)
          raise Exception(f"Unable to write to file {self.full_output_path_to_clean_text} while using arguments {self.full_output_path_to_clean_text, output_file_strings}")
    except :
      raise Exception(f'Error loading dataframe for cleaning at {self.full_input_path_to_text_to_clean}')

# DEMO TIME! 
if __name__ == "__main__" : 
  null_map = {} 
  null_map['""'] = '""'
  null_map["''"] = "''"
  
  demo_cleaner = text_cleaner('comparisonvalues.csv', 'target_clc.csv', input_file_delimiter=';', 
                         output_file_delimiter=';', header=None, 
                         index_col=False, na_values=["NULL", "null"], 
                         keep_default_na=False, 
                         quote_char = '"', escape_char = '\\', 
                         on_bad_lines='warn', empty_string_map=null_map, 
                         not_null_map=null_map, 
                         additional_field_map={"\\N":'""'}, 
                         remove_metatags=True, metatag_replacement='',
                         spacing_between_items_in_fields=1,
                         string_notation_char = '"')
  demo_cleaner.attempt_load_text_to_dataframe()
  demo_cleaner.clean_dataframe_to_text_file()

  

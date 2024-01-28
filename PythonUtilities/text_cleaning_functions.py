import re
contains_metatag = re.compile('<.*?>')

def remove_metatags_from_string(string_to_clean, replace_target='') : 
  if len(metatags_contained := contains_metatag.findall(string_to_clean)) : 
    for tag in metatags_contained : 
      string_to_clean = string_to_clean.replace(tag, replace_target)
  return string_to_clean

def replace_empty_string_with_target_string(string_to_clean, replace_target='""') : 
  if len(string_to_clean) == 0 : 
    string_to_clean = replace_target
  return string_to_clean

def replace_source_string_with_target_string(string_to_clean, target_to_replace='\n', replace_target='""') : 
  if len(string_to_clean) == 0 : 
    return replace_empty_string_with_target_string(string_to_clean, replace_target=replace_target)
  else : 
    string_to_clean = string_to_clean.replace(target_to_replace, replace_target)
    return string_to_clean

def prepend_string_if_missing(string_to_clean, prepend_character='"') : 
  if string_to_clean[0] != prepend_character :
    string_to_clean = prepend_character + string_to_clean
  return string_to_clean

def append_string_if_missing(string_to_clean, append_character='"') :
  if string_to_clean[-1] != append_character :
    string_to_clean = string_to_clean + append_character
  return string_to_clean

def mutate_multi_lines_and_spacings_to_single_line_set_spaces(string_to_clean, number_of_spaces=1) : 
  string_to_clean = string_to_clean.replace('\n', ' ')
  string_to_clean = string_to_clean.replace('\r', ' ')
  string_to_clean = string_to_clean.replace('\t', ' ')
  string_to_clean_array = string_to_clean.split()
  space_joiner = ' ' * number_of_spaces if number_of_spaces != 0 else ''
  string_to_clean = space_joiner.join(string_to_clean_array)
  return string_to_clean

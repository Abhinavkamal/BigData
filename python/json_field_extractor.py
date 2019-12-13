import re
import json

# how to call
# generate_key_paths(dic, ['bestParse'], 0) -- for first occurence
# generate_key_paths(dic, ['bestParse'], -1) -- for first occurence
# generate_key_paths(dic, ['bestParse', 'verbLabels'], 0) -- for first occurence and filter whih specifies parent key for verbLabels as bestParse
# generate_key_paths(dic, ['bestParse']) -- for all occurences
#gives last occurence for all other output types.

def get_items(root, *keys):
   """
   This method checks if the given key or keys[for a nested element] exists in the input dictionary and
    returns the value for the last element in keys.
   :param root: input dictionary
   :param keys: list of keys
   :return: element if last element of keys is found in root, otherwise None
   """
   root = root
   if type(root) is list:
       if type(keys[0]) is int:
           root = root[keys[0]]
           keys = keys[1:]
   if not keys:
       return None
   if keys[0] not in root:
       return None
   if ((keys[0] in root) & (len(keys) > 1)):
       return get_items(root[keys[0]], *keys[1:])
   elif ((keys[0] in root) & (len(keys) == 1)):
       output = root.get(keys[0],None)
       return output
   else:
       return None



def fetch_data_from_dict(input_dict, fields_list):
   return get_items(input_dict, *fields_list)

def recursive_items(input_list, prev_item,  dictionary):
   """
   Its a recursive method to find all the keys until the lowest level.
   :param input_list: []
   :param prev_item: a string value which is used in recursion check
   :param dictionary: input dictionary
   :return: list of keys for all the items in the dictionary.
   """
   for key, value in dictionary.items():
       # input_check_list = [] if len(input_list) == 0  else input_list
       # prev = None if len(input_list) == 0 else input_list[-1]
       if prev_item in input_list:
           index = input_list.index(prev_item)
           internal = input_list[0:index+1]
       else:
           internal = []
       if type(value) is dict:
           internal.append(key)
           yield from recursive_items(internal, key, value)
       elif type(value) is list:
           internal.append(key)
           if len(value) == 0:
               yield internal
               pass
           else:
               for item in value:
                   if type(item) in [dict, list]:
                       internal.append(value.index(item))
                       yield from recursive_items(internal, value.index(item), item)
                       internal = internal[:-1]
                   else:
                       yield internal
       else:
           internal.append(key)
           yield internal


def check_input_dict(inpt_dct):
   if type(inpt_dct) is dict:
       return inpt_dct
   elif type(inpt_dct) is str:
       try:
           return json.loads(inpt_dct)
       except Exception as e:
           return {}


def generate_key_paths(input_dict, check_key, output_index=None):
   """
   Takes a list of keys and fetches the values which can be located in any level in the entire dictionary.
   :param input_dict: dictionary
   :param check_key: list of keys. function will fetch the value for last item in the list, but use other elements to filter.
   :param output_type: list- list gives all occurences of the keys, first - gives only the first occurence of key.
   :return: returns value based on output type param.
   """
   inpt_dict = check_input_dict(input_dict)
   recr_output = list(recursive_items([], None, inpt_dict))
   for key in check_key:
       recr_output = [i for i in recr_output if key in i]
   keys_list = []
   if len(recr_output) > 0:
       for item in recr_output:
           indx = recr_output[recr_output.index(item)].index(check_key[-1])
           if not recr_output[recr_output.index(item)][0:indx+1] in keys_list:
               keys_list.append( recr_output[recr_output.index(item)][0:indx+1])
   if len(keys_list) == 0:
       return None
   else:
       values_list = [fetch_data_from_dict(inpt_dict, key) for key in keys_list]
       if output_index is None:
           return values_list
       else:
           return values_list[output_index]

inp = '{"p":[{"l":"default","i":"hey"},{"l":"default","i":"siri"},{"l":"answerMiscQuestion","i":"what\'s"},{"l":"answerMiscQuestion","i":"the"},{"l":"answerMiscQuestion","i":"temperature"},{"l":"answerMiscQuestion","i":"of"},{"l":"answerMiscQuestion","i":"softball"},{"l":"answerMiscQuestion","i":"stage"}],"d":"answerFacts","v":"find"}'


generate_key_paths(inp, ['p','i'])

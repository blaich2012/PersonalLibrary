'''
  By Brian Laich, Data Engineer and AI/ML Engineer
  Showcases basic decorators in python for use in other functions
  These can then be utilized in other areas via recoding to your purposes
'''
import builtins
import time 
import sqlite3
default_print_line = builtins.print

'''
  Allows set up of logging via passed function 
  Utilizes function to decorate as the function to decorate on 
  Utilizes wrapper as wrapper function to call 
  Aliases default_print_line from builtins print if not assigned 
'''
def log_decorator_function(function_to_decorate, function_to_log_with=default_print_line) : 
  def wrapper(*args, **kwargs): 
    arugment_string_form = f' function to decorate with arguments : {args}'
    keyword_arguments_string_form = f' and keyword arguments : {kwargs} \n'
    start_string = f' the function {function_to_decorate.__name__}'
    combined_string = start_string + argument_string_form + keyword_arguments_string_form
    function_to_log_with.print(f"Before {combined_string}")
    result = function_to_decorate(*args, **kwargs)
    function_to_log_with.print(f"Result of {combined_string} returned {result}")
    return result 
  return wrapper 

# Example setup below 
'''
@log_decorator
def calculate_product(x, y) : 
  return x*y

result = calculate_product(10, 20)
print(f"Resulting value is {result}")
'''

def measure_execution_time_decorator_function(function_to_decorate, function_to_log_with=default_print_line) : 
  def time_of_execution(*args, **kwargs) : 
    start_timestamp = time.time()
    result = function_to_decorate(*args, **kwargs)
    end_timestamp = time.time()
    execution_duration = end_timestamp - start_timestamp 
    arugment_string_form = f' function to decorate with arguments : {args}'
    keyword_arguments_string_form = f' and keyword arguments : {kwargs} \n'
    start_string = f' the function {function_to_decorate.__name__}'
    combined_string = start_string + argument_string_form + keyword_arguments_string_form
    function_to_log_with.print(f"Run time of {combined_string} is {execution_duration:.2f} seconds.")
    return result
  return time_of_execution

# Example setup below
'''
@measure_execution_time_decorator_function
def multiply_numbers(numbers) : 
  product = 1
  for num in numbers : 
    product *= num 
  return product 

result = multiply_numbers([i for i in range(1, 10)])
print(f"Result is {result}")
'''

def convert_to_data_type_decorator_function(target_type) : 
  def type_conversion_decorator(function_to_decorate) : 
    def wrapper(*args, **kwargs) : 
      result = function_to_decorate(*args, **kwargs)
      return target_type(result)
    return wrapper 
  return convert_to_data_type_decorator_function

# Example setup below
'''
@convert_to_data_type_decorator_function(int)
def calculate_product(x, y) : 
  return x * y 

int_result = calculate_product(10, 20)
print(f"Result is {int_result} with typing of {type(int_result)}")

@convert_to_data_type_decorator_function(string)
def concatenate_strings(string_1, string_2) : 
  return string_1 + string_2

string_result = concatenate_strings("Python ", "Decorator")
print(f"Result is {string_result} with typing of {type(string_result)}")
'''

def cached_results_decorator_function(function_to_decorate) : 
  result_cache = {} 
  def wrapper(*args, **kwargs) : 
    if (cache_keys := (*args, *kwargs.items())) in result_cache : 
      # comment following line to improve performance
      print("Result was in cache")
      return result_cache.get(cache_keys)
    else : 
      # comment following line to improve performance 
      print("Result was not in cache")
      result = function_to_decorate(*args, **kwargs)
      result_cache[cache_key] = result 
      return result_cache.get(cache_key) 
  return wrapper 

# Example Usage 
'''
@cached_results_decorator_function
def calculate_product(x, y) : 
  return x * y 

i = 1
j = 2 
for i in range(1, 5) : 
  for j in range(2, 10) :
    calculate_product(i, j) 
'''

def cached_results_decorator_function_with_expiration_time(expiry_time_seconds=60) : 
  def decorator_function(function_to_decorate) : 
    results_cache = {} 
    def wrapper_for_decoration_function(*args, **kwargs) : 
      if (result_key := (*args, *kwargs.items())) in results_cache : 
        cached_result, cached_timestamp = results_cache.get(result_key)
        if time.time() - cached_timestamp < expiry_time_seconds : 
          return cached_result 
      current_result = function_to_decorate(*args, **kwargs)
      results_cache[result_key] = (current_result, time.time())
      return current_result
    return wrapper_for_decoration_function
  return decorator_function

# Example below 
'''
"""
  This example showcases using a cached results decorator function with expiry to 
  establish a connection  to the database wherein if the result is already cached
  then we can skip the connection to the database and return the result 
  Otherwise, if we are over the expiry time seconds since last, we can just 
  pop in, query, and return the result. 
  We'll try this 3 times by the other function below to get the result. 
  We'll do this at the specified frequency, defaulted to one day in seconds 
  We'll start one frequency cycle after the stated start time if any or simply when called. 
"""

@cached_results_decorator_function_with_expiration_time(expiry_time_seconds=900) 
def establish_database_connection(target_database, sql_to_execute) : 
  connection=sqlite3.connect(target_database)
  db_cursor = connection.cursor()
  db_cursor.execute(sql_to_execute)
  query_result = db_cursor.fetchall()
  db_cursor.close()
  connection.close()
  return query_result

@retry_on_failure_function_decorator(max_attempts=3, retry_delay=10)
def generate_query_results(target_database, sql_to_execute) : 
  default_database_target_string = f" to establish database connection using {target_database} "
  default_sql_execution_string = f" with sql command of {sql_to_execute}"
  try: 
    retrieved_data = establish_database_connection(target_database, sql_to_execute)
    return retrieved_data
  except Exception as error_message: 
    print(f"Failed{default_database_target_string}{default_sql_execution_string} resulting in {error_message}")

def run_on_schedule_frequency(start_time = None, frequency=3600*24, target_database, sql_to_execute) : 
  schedule_state = True
  while schedule_state : 
    if start_time is None : 
      time.sleep(1)
      try : 
        result = generate_query_results(target_database, sql_to_execute)
        yield result 
        start_time = time.time() 
      except Exception as error: 
        print(f"Error processing request : {error_message}. Shutting down process.")
        schedule_state = False
    else : 
      next_time = start_time + frequency
      time.sleep(next_time)
      try : 
        result = generate_query_results(target_database, sql_to_execute) 
        yield result
        start_time = time.time()
      except Exception as error : 
        print(f"Error processing request : {error_message}. Shutting down process.")
        schedule_state = False 
'''
        
def check_input_numeric_values_follow_lambda_pattern_for_function_to_decorate(value) : 
  def argument_validation_wrapper(function_to_decorate) : 
    def validate_and_calculate(*args, **kwargs) : 
      if value(*args, **kwargs) : 
        return function_to_decorate(*args, **kwargs) 
      else : 
        start = "Invalid arguments in "
        components = f"{args} or {kwargs} passed to {function_to_decorate.__name__}. "
        validator = f"Failed on value check with {value}."
        raise ValueError(f"{start}{components}{validator}")
    return validate_and_calculate
  return argument_validation_wrapper

# Example Below 
'''
@check_input_numeric_values_follow_lambda_pattern_for_function_to_decorate(lambda numeric_argument : numeric_argument > 0)
def compute_third_power(numeric_argument) : 
  return numeric_argument**3

print(f"Using 5,  the result of our compute third power function is {compute_third_power(5)}")
print(f"Using -2, the result of our compute third power function is {compute_third_power(-2)}")
'''

def retry_on_failure_function_decorator(max_attempts=1, retry_delay=1) : 
  def function_to_retry(function_to_decorate): 
    def decoration_wrapper(*args, **kwargs) : 
      arugment_string_form = f' function to decorate with arguments : {args}'
      keyword_arguments_string_form = f' and keyword arguments : {kwargs} \n'
      start_string = f' the function {function_to_decorate.__name__}'
      combined_string = start_string + argument_string_form + keyword_arguments_string_form
      for i in range(max_attempts) : 
        try : 
          result = function_to_decorate(*args, **kwargs)
          return result 
        except Exception as error : 
          print(f"Error occurred while attempting{combined_string}. This is retry {i+1}")
          time.sleep(retry_delay)
      raise Exception(f"Maximum attempts exceeded while attempting{combined_string}. Function fails")
    return decoration_wrapper
  return function_to_retry

# Example Below
'''
@retry_on_failure_function_decorator(max_attempts=3, retry_delay=10)
def establish_database_connection(target_database, sql_to_execute):
  connection=sqlite3.connect(target_database)
  db_cursor = connection.cursor()
  db_cursor.execute(sql_to_execute)
  query_result = db_cursor.fetchall()
  db_cursor.close()
  connection.close()
  return query_result

database_targeted = "example.db"
desired_sql_to_execute = "select * from users limit 10"
default_database_target_string = f" to establish database connection using {database_targeted} "
default_sql_execution_string = f" with sql command of {desired_sql_to_execute}"


try: 
  retrieved_data = establish_database_connection(database_targeted, desired_sql_to_execute)
  print(f"Managed{default_database_target_string}{default_sql_execution_string}")
  print(f"Data retrieved successfully as {retrieved_data}")
except Exception as error_message:
  print(f"Failed{default_database_target_string}{default_sql_execution_string} resulting in {error_message}.")
'''

def exponential_delay_retry_on_failure_function_decorator(max_attempts=1, retry_delay_start=1, retry_delay_exponential_multiple=2) : 
  def function_to_retry(function_to_decorate) : 
    def decoration_wrapper(*args, **kwargs) : 
      arugment_string_form = f' function to decorate with arguments : {args}'
      keyword_arguments_string_form = f' and keyword arguments : {kwargs} \n'
      start_string = f' the function {function_to_decorate.__name__}'
      combined_string = start_string + argument_string_form + keyword_arguments_string_form
      retry_delay = retry_delay_start
      message_components = [""]*3
      message_components[0] = f"Error occurred while attempting {combined_string}."
      for retry_count in range(max_attempts) : 
        next_retry_delay = retry_delay * retry_delay_exponential_multiple
        message_components[1] = f" This is retry {retry_count + 1} of {max_attempts}."
        message_components[2] = f" Delay until next retry is {next_retry_delay} in seconds."
        retry_delay = next_retry_delay
        try : 
          result = function_to_decorate(*args, **kwargs)
          return result 
        except Exception as error : 
          print(''.join(message_components))
          time.sleep(retry_delay)
      raise Exception("Maximum attempts {max_attempts} exceeded for {combined_string}. Failing {start_string}")
    return decoration_wrapper
  return function_to_retry

# Example below 
'''
@exponential_delay_retry_on_failure_function_decorator(max_attempts=3, retry_delay_start=10, retry_delay_exponential_multiple=10)
def establish_database_connection(target_database, sql_to_execute) : 
  connection=sqlite3.connect(target_database)
  db_cursor = connection.cursor()
  db_cursor.execute(sql_to_execute)
  query_result = db_cursor.fetchall()
  db_cursor.close()
  connection.close()
  return query_result

database_targeted = "example.db"
desired_sql_to_execute = "select * from users limit 10"
default_database_target_string = f" to establish database connection using {database_targeted} "
default_sql_execution_string = f" with sql command of {desired_sql_to_execute}"

try: 
  retrieved_data = establish_database_connection(database_targeted, desired_sql_to_execute)
  print(f"Managed{default_database_target_string}{default_sql_execution_string}")
  print(f"Data retrieved successfully as {retrieved_data}")
except Exception as error_message:
  print(f"Failed{default_database_target_string}{default_sql_execution_string} resulting in {error_message}.")
'''

def rate_limit_function_decorator(max_allowed_calls=10, reset_period_seconds=10) : 
  def decorate_rate_limited_function(function_to_decorate) : 
    calls_count = 0
    last_reset_time = time.time()
    def wrapper_for_rate_limiting(*args, **kwargs) : 
      nonlocal calls_count, last_reset_time
      elapsed_time = time.time() - last_reset_time
      if elapsed_time > reset_period_seconds : 
        calls_count = 0
        last_reset_time = time.time() 
      if calls_count >= max_allowed_calls : 
        raise Exception(f"Rate limit of {max_allowed_calls} exceeded at call count of {calls_count}. Try again in {reset_period_seconds * 2} seconds")
      calls_count += 1 
      return function_to_decorate(*args, **kwargs)
    return wrapper_for_rate_limiting
  return decorate_rate_limited_function

# Example Below 
'''
@rate_limit_function_decorator(max_allowed_calls=5, reset_period_seconds=10) 
def make_api_call() : 
  print(f"API call executed successfully.")

number_of_calls_to_make = 8
for call_count in range(number_of_calls_to_make) : 
  try : 
    make_api_call()
  except Exception as error: 
    print(f"Error occurred making api call : {error}")
time.sleep(10)
make_api_call()
'''

def handle_exceptions(default_response_message="Please address the above error before retrying.") : 
  def exception_handler_decorator(function_to_decorate) : 
    def decorated_function_wrapper(*args, **kwargs) : 
      try : 
        return function_to_decorate(*args, **kwargs) 
      except Exception as error: 
        print(f"An error occurred while processing your request : {error}")
        return default_response_message
    return decorated_function_wrapper
  return exception_handler_decorator

# Example below 
'''
@handle_exceptions()
def divide_numbers_safely(dividend, divisor) : 
  return dividend/divisor

result = divide_numbers_safely(7, 0)
print(f"Result produced is {result}")
'''
          
          
        
    
    
    
    

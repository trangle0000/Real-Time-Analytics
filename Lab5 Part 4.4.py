
print("="*80)
print("Task 4.4: Review Questions & Answers")
print("="*80)

questions_answers = """
QUESTION 1: What is the difference between GET and POST?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ANSWER:
  GET:
    • Retrieves data from server
    • No request body (data in URL)
    • Safe operation (doesn't modify data)
    • Example: GET /hello?name=Anna
    
  POST:
    • Sends data to server for processing
    • Request body contains JSON data
    • Creates or modifies data
    • Example: POST /score with transaction JSON

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QUESTION 2: Why use jsonify() instead of return {"key": "value"}?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ANSWER:
  jsonify():
    • Sets correct HTTP headers: Content-Type: application/json
    • Properly serializes Python dict to JSON format
    • Client knows to parse response as JSON
    • Handles nested objects and special types
    
  return {"key": "value"}:
    • Returns Python dict string representation
    • Missing proper headers
    • Client doesn't know it's JSON
    • May cause parsing errors

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QUESTION 3: What happens if two people call /score at the same time?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ANSWER:
  Flask handles concurrency:
    • Each request gets its own thread
    • Each request gets own execution context
    • Requests process simultaneously
    
  Global counters problem:
    • Counter updates are NOT thread-safe
    • Two threads may read/write same counter value
    • Results: Lost updates or wrong counts
    
  Solution for production:
    • Use threading.Lock() to protect counter
    • Use database instead of global variables
    • Use Redis or queue service

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

print(questions_answers)

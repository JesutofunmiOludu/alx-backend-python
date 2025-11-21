from datetime import datetime
from django.contrib.auth.models import AnonymousUser

class RequestLoggingMiddleware(object):
    """
    Middleware that logs each incoming request's method and path.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Log the request method and path
        user = request.user if not isinstance(request.user, AnonymousUser) else 'Anonymous'
        username = user.username if user != 'Anonymous' else 'Anonymous'
        log_file = f"{datetime.now()} - User: {username} - Path: {request.path}" 
       
        with open('request_logs.txt', 'a') as log_file:
            log_file.write(log_file + '\n')

        response = self.get_response(request)
        return response

class RestrictAccessByTimeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
    
    def __call__(self, request):
        current_time = datetime.now().hour()
        # Restrict access to certain hours (e.g., 9 AM to 5 PM)
        if not (9 <= current_time.hour < 17):
            from django.http import HttpResponseForbidden
            return HttpResponseForbidden("Access is restricted to business hours (9 AM to 5 PM).")
        
        response = self.get_response(request)
        return response
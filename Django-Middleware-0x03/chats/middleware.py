from datetime import datetime
from django.contrib.auth.models import AnonymousUser
from django.http import HttpResponseForbidden
from django.http import HttpResponseTooManyRequests

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

class OffensiveLanguageMiddleware:
    RATE_LIMIT = 5  # Max messages allowed
    TIME_WINDOW_SECONDS = 60

    def __init__(self, get_response):
        self.request_log = {}
        self.get_response = get_response

    def __call__(self, request):

        ip_address = request.META.get('REMOTE_ADDR')
        current_time = datetime.now().timestamp()
        if ip_address not in self.request_log:
            self.request_log[ip_address] = []
        cutoff_time = current_time - self.TIME_WINDOW_SECONDS
        self.request_log[ip_address] = [
            t for t in self.request_log[ip_address] if t > cutoff_time]
        request_count = len(self.request_log[ip_address])
        if request_count >= self.RATE_LIMIT:
            
            return HttpResponseTooManyRequests("Too many requests. Please try again later.")
        else:
            self.request_log[ip_address].append(current_time)
        response = self.get_response(request)
        return response

class RolePermissionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.restricted_paths = {
            '/admin/': ['admin'],
            '/moderator/': ['admin', 'moderator'],
        }

    def __call__(self, request):
        if request.path in self.restricted_paths:
            if not request.user.is_authenticated:
               
                return HttpResponseForbidden("You must be logged in to access this page.")
            user_role = getattr(request.user, 'role', 'user')
            is_admin = user.is_active and user.is_superuser
            is_moderator = user.groups.filter(name='moderator').exists()
            if not(is_admin or is_moderator):
                
                return HttpResponseForbidden("Error 403 Forbidden:You do not have permission to access this page.")
        
        response = self.get_response(request)
        return response
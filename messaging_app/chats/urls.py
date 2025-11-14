from django.urls import path, include, DefaultRouter
from rest_framework.routers import DefaultRouter
from .views import ConversationViewSet, MessageViewSet, UserViewSet

# Create a router and register our viewsets
router = routers.DefaultRouter()
router.register(r'conversations', ConversationViewSet, basename='conversation')
router.register(r'messages', MessageViewSet, basename='message')
router.register(r'users', UserViewSet, basename='user')

# The API URLs are determined automatically by the router
urlpatterns = [
    path('', include(router.urls)),
]
from django.shortcuts import render
from rest_framework import viewsets
from .models import User, Conversation, Message
from .serializers import UserSerializer, ConversationSerializer, MessageSerializer, ConversationListSerializer
from rest_framework.permissions import IsAuthenticated
from .permissions import IsConversationParticipant
from django.views.decorators.cache import cache_page


# Create your views here.
class ConversationViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]
    def get_permissions(self):
        permission = [IsAuthenticated]
        if self.action in ['retrieve', 'update', 'partial_update', 'destroy', 'add_participant', 'remove_participant']:
            permission.append(IsConversationParticipant())

        return permission
    
    def get_queryset(self):
        user = self.request.user
        return Conversation.objects.filters(participants=user).prefetch_related('participants', 'messages')
    def get_serializer_class(self):
        if self.action == 'list':
            return ConversationListSerializer
        return ConversationSerializer
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        
        return Response(serializer.data)
    def create(self, request, *args, **kwargs):
        """
        Create a new conversation.
        POST /conversations/
        
        Request body:
        {
            "participant_ids": ["uuid1", "uuid2", "uuid3"]
        }
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        # Automatically add the authenticated user as a participant
        participant_ids = request.data.get('participant_ids', [])
        if request.user.user_id not in participant_ids:
            participant_ids.append(str(request.user.user_id))
        
        # Update the data with the current user included
        data = request.data.copy()
        data['participant_ids'] = participant_ids
        
        serializer = self.get_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        conversation = serializer.save()
        
        # Return full conversation details
        response_serializer = ConversationSerializer(conversation)
        return Response(
            response_serializer.data,
            status=status.HTTP_201_CREATED
        )
    
    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve a specific conversation with all messages.
        GET /conversations/{id}/
        """
        conversation = self.get_object()
        serializer = ConversationSerializer(conversation)
        return Response(serializer.data)
    
    def update(self, request, *args, **kwargs):
        """
        Update a conversation (e.g., add/remove participants).
        PUT /conversations/{id}/
        PATCH /conversations/{id}/
        """
        partial = kwargs.pop('partial', False)
        conversation = self.get_object()
        serializer = self.get_serializer(
            conversation,
            data=request.data,
            partial=partial
        )
        serializer.is_valid(raise_exception=True)
        
        # Handle participant updates if provided
        participant_ids = request.data.get('participant_ids')
        if participant_ids:
            users = User.objects.filter(user_id__in=participant_ids)
            conversation.participants.set(users)
        
        conversation.save()
        
        response_serializer = ConversationSerializer(conversation)
        return Response(response_serializer.data)
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete a conversation.
        DELETE /conversations/{id}/
        """
        conversation = self.get_object()
        conversation.delete()
        return Response(
            {"message": "Conversation deleted successfully"},
            status=status.HTTP_204_NO_CONTENT
        )
    
#    @action(detail=True, methods=['post'])
    def add_participant(self, request, pk=None):
        """
        Add a participant to an existing conversation.
        POST /conversations/{id}/add_participant/
        
        Request body:
        {
            "user_id": "uuid-of-user-to-add"
        }
        """
        conversation = self.get_object()
        user_id = request.data.get('user_id')
        
        if not user_id:
            return Response(
                {"error": "user_id is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response(
                {"error": "User not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Check if user is already a participant
        if conversation.participants.filter(user_id=user_id).exists():
            return Response(
                {"error": "User is already a participant"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Add participant
        conversation.participants.add(user)
        
        serializer = ConversationSerializer(conversation)
        return Response(serializer.data)
    
    #@action(detail=True, methods=['post'])
    def remove_participant(self, request, pk=None):
        """
        Remove a participant from a conversation.
        POST /conversations/{id}/remove_participant/
        
        Request body:
        {
            "user_id": "uuid-of-user-to-remove"
        }
        """
        conversation = self.get_object()
        user_id = request.data.get('user_id')
        
        if not user_id:
            return Response(
                {"error": "user_id is required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response(
                {"error": "User not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Check if user is a participant
        if not conversation.participants.filter(user_id=user_id).exists():
            return Response(
                {"error": "User is not a participant"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Prevent removing if only 2 participants left
        if conversation.participants.count() <= 2:
            return Response(
                {"error": "Cannot remove participant. Minimum 2 participants required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Remove participant
        conversation.participants.remove(user)
        
        serializer = ConversationSerializer(conversation)
        return Response(serializer.data)


class MessageViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing messages.
    
    Endpoints:
    - GET /messages/ - List all messages for the authenticated user
    - POST /messages/ - Send a new message
    - GET /messages/{id}/ - Retrieve a specific message
    - PUT /messages/{id}/ - Update a message (edit)
    - DELETE /messages/{id}/ - Delete a message
    - GET /messages/conversation/{conversation_id}/ - Get all messages in a conversation
    """
    serializer_class = MessageSerializer
    permission_classes = [IsAuthenticated, IsConversationParticipant]
    
    def get_queryset(self):
        """
        Return only messages from conversations the authenticated user is part of.
        """
        user = self.request.user
        return Message.objects.filters(
            conversation__participants=user
        ).select_related('sender', 'conversation').distinct()
    
    def list(self, request, *args, **kwargs):
        """
        List all messages for the authenticated user across all conversations.
        GET /messages/
        
        Optional query parameters:
        - conversation_id: Filter messages by conversation
        """
        queryset = self.get_queryset()
        
        # Filter by conversation if provided
        conversation_id = request.query_params.get('conversation_id')
        if conversation_id:
            queryset = queryset.filter(conversation_id=conversation_id)
        
        # Order by most recent first
        queryset = queryset.order_by('-sent_at')
        
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    
    def create(self, request, *args, **kwargs):
        """
        Send a new message to a conversation.
        POST /messages/
        
        Request body:
        {
            "conversation": "conversation-uuid",
            "message_body": "Hello, world!"
        }
        
        Note: sender_id is automatically set to the authenticated user
        """
        # Add the authenticated user as the sender
        data = request.data.copy()
        data['sender_id'] = str(request.user.user_id)
        
        serializer = self.get_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        message = serializer.save()
        
        return Response(
            serializer.data,
            status=status.HTTP_201_CREATED
        )
    
    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve a specific message.
        GET /messages/{id}/
        """
        message = self.get_object()
        serializer = self.get_serializer(message)
        return Response(serializer.data)
    
    def update(self, request, *args, **kwargs):
        """
        Update a message (edit message body).
        PUT /messages/{id}/
        PATCH /messages/{id}/
        
        Request body:
        {
            "message_body": "Updated message text"
        }
        """
        message = self.get_object()
        
        # Only allow the sender to edit their own messages
        if message.sender.user_id != request.user.user_id:
            return Response(
                {"error": "You can only edit your own messages"},
                status=status.HTTP_403_FORBIDDEN
            )
        
        partial = kwargs.pop('partial', False)
        serializer = self.get_serializer(
            message,
            data=request.data,
            partial=partial
        )
        serializer.is_valid(raise_exception=True)
        
        # Only allow updating message_body
        if 'message_body' in request.data:
            message.message_body = request.data['message_body']
            message.save()
        
        response_serializer = self.get_serializer(message)
        return Response(response_serializer.data)
    
    def destroy(self, request, *args, **kwargs):
        """
        Delete a message.
        DELETE /messages/{id}/
        """
        message = self.get_object()
        
        # Only allow the sender to delete their own messages
        if message.sender.user_id != request.user.user_id:
            return Response(
                {"error": "You can only delete your own messages"},
                status=status.HTTP_403_FORBIDDEN
            )
        
        message.delete()
        return Response(
            {"message": "Message deleted successfully"},
            status=status.HTTP_204_NO_CONTENT
        )
    
    #@action(detail=False, methods=['get'], url_path='conversation/(?P<conversation_id>[^/.]+)')
    def by_conversation(self, request, conversation_id=None):
        """
        Get all messages in a specific conversation.
        GET /messages/conversation/{conversation_id}/
        """
        # Verify the user is a participant in this conversation
        try:
            conversation = Conversation.objects.get(
                conversation_id=conversation_id,
                participants=request.user
            )
        except Conversation.DoesNotExist:
            return Response(
                {"error": "Conversation not found or you are not a participant"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        messages = self.get_queryset().filter(
            conversation=conversation
        ).order_by('sent_at')
        
        serializer = self.get_serializer(messages, many=True)
        return Response(serializer.data)


class UserViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing users.
    
    Endpoints:
    - GET /users/ - List all users (admin only)
    - POST /users/ - Register a new user
    - GET /users/{id}/ - Retrieve a specific user
    - PUT /users/{id}/ - Update a user
    - DELETE /users/{id}/ - Delete a user
    """
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """
        Regular users can only see themselves.
        Admins can see all users.
        """
        user = self.request.user
        if user.role == 'admin':
            return User.objects.all()
        return User.objects.filter(user_id=user.user_id)
    
    def create(self, request, *args, **kwargs):
        """
        Register a new user.
        POST /users/
        
        Request body:
        {
            "email": "user@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "password": "SecurePass123!",
            "password_confirm": "SecurePass123!",
            "role": "guest"
        }
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        
        return Response(
            {
                "user_id": user.user_id,
                "email": user.email,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "role": user.role,
                "created_at": user.created_at
            },
            status=status.HTTP_201_CREATED
        )

@cache_page(60)  # Cache the view for 15 minutes
def conversation_view(request, conversation_id):
    messages = Message.objects.filter(parent_message__isnull = True, 
                                     reciever = request.user
                                     ).select_related('sender').prefetch_related('replies')
    return render(request, 'chats/conversation.html', {
        "messages": messages
    })
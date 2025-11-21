from rest_framework import serializers
from .models import User, Conversation, Message
from django.contrib.auth.password_validation import validate_password

class UserSerializer(serializers.ModelSerializer):
    password1 = serializers.CharField(write_only=True, required=True, 
                                     validators=[validate_password], style ={'input_type': 'password'})
    password2 = serializers.CharField(write_only=True, required=True, style ={'input_type': 'password'})

    class Meta:
        model = User
        fields = ['user_id', 'first_name', 'last_name', 'email', 'password1', 'password2']

    read_only_fields = ['user_id', 'created_at', 'updated_at']
    extra_kwargs = {
        'first_name': {'required': True},
        'last_name': {'required': True},
        'email': {'required': True},
    }
    def validate(self, attrs):
        if attrs['password1'] != attrs['password2']:
            raise serializers.ValidationError({"password": "Password fields didn't match."})
        return attrs
    def create(self, validated_data):
        validated_data.pop('password2')
        user = User(
            username=validated_data['email'],
            first_name=validated_data['first_name'],
            last_name=validated_data['last_name'],
            email=validated_data['email'],
            phone_number=validated_data.get('phone_number'),
            password=validated_data.get['password'],
            role=validated_data.get('role', User.Role.GUEST)
        )
        user.set_password(validated_data['password1'])
        user.save()
        return user
    def update(self, instance, validated_data):
        password = validated_data.pop('password1', None)
        validated_data.pop('password2', None)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        if password:
            instance.set_password(password)
        instance.save()
        return instance
class UserBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['user_id', 'first_name', 'last_name', 'email']
        read_only_fields = ['user_id', 'first_name', 'last_name', 'email']

class MessageSerializer(serializers.ModelSerializer):
    sender = UserBasicSerializer(read_only=True)

    class Meta:
        model = Message
        fields = ['message_id', 'conversation', 'sender', 'content', 'timestamp']
        read_only_fields = ['message_id', 'timestamp']
    def validate_sender_id(self, value):
        if not User.objects.filter(user_id=value).exists():
            raise serializers.ValidationError("Sender with the given ID does not exist.")
        return value
    def validate(self, attrs):
        conversation= attrs.get('conversation')
        sender_id = attrs.get('sender_id')

        if conversation and sender_id:
            is_participant = conversation.participants.filter(user_id=sender_id).exists()
            if not is_participant:
                raise serializers.ValidationError("Sender is not a participant in the conversation.")
            
        return attrs
    
    def create(self, validated_data):
        sender_id = validated_data.pop('sender_id')
        sender = User.objects.get(user_id=sender_id)
        validated_data['sender'] = sender

        return super().create(validated_data)
class ConversationParticipantSerializer(serializers.ModelSerializer):
    user = UserBasicSerializer(read_only=True)

    class Meta:
        model = Conversation.participants.through
        fields = ['id','user_id','conversation', 'user', 'joined_at']
        read_only_fields = ['id', 'joined_at']  

class ConversationSerializer(serializers.ModelSerializer):
    participants = UserBasicSerializer(many=True, read_only=True)
    participant_ids = serializers.ListField(
        child=serializers.UUIDField(), write_only=True, required=False
    )

    class Meta:
        model = Conversation
        fields = ['conversation_id', 'participants', 'participant_ids','messages', 'message_count', 'created_at']
        read_only_fields = ['conversation_id', 'created_at','participants','messages']
    def get_message_count(self, obj):
        return obj.messages.count()
    def get_participant_count(self, obj):
        return obj.participants.count()
    def validate_participant_ids(self, value):
        if len(value) < 2:
            raise serializers.ValidationError("At least two participants are required to create a conversation.")
        existing_users = User.objects.filter(user_id__in=value).values_list('user_id', flat=True)
        if existing_users.count() != len(set(value)):
            raise serializers.ValidationError("One or more participant IDs are invalid.")
        return value
    def create(self, validated_data):
        participant_ids = validated_data.pop('participant_ids', [])
        conversation = Conversation.objects.create(**validated_data)
        for user_id in participant_ids:
            user = User.objects.get(user_id=user_id)
            conversation.participants.add(user)
        return conversation
    
class ConversationListSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for listing conversations without all messages.
    Shows only the last message for preview.
    """
    participants = UserBasicSerializer(many=True, read_only=True)
    participant_count = serializers.SerializerMethodField()
    last_message = serializers.SerializerMethodField()
    unread_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Conversation
        fields = [
            'conversation_id',
            'participants',
            'participant_count',
            'last_message',
            'unread_count',
            'created_at'
        ]
        read_only_fields = fields
    
    def get_participant_count(self, obj):
        """
        Get the total number of participants.
        """
        return obj.participants.count()
    
    def get_last_message(self, obj):
        """
        Get the most recent message in the conversation.
        """
        last_message = obj.messages.order_by('-sent_at').first()
        if last_message:
            return {
                'message_id': last_message.message_id,
                'sender': UserBasicSerializer(last_message.sender).data,
                'message_body': last_message.message_body[:100],  # Preview only
                'sent_at': last_message.sent_at
            }
        return None
    
    def get_unread_count(self, obj):
        """
        Placeholder for unread message count.
        Would require additional tracking in a real implementation.
        """
        # TODO: Implement read status tracking
        return 0
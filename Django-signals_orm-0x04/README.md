# README

## Overview
In modern web applications, performance, modularity, and clean architecture are essential. Django provides powerful tools that help developers build robust and maintainable backend systems. Three core concepts that support these goals are:

### **Event Listeners using Django Signals**
Signals allow decoupled parts of an application to communicate by emitting and listening to events. This enables actions like sending confirmation emails or logging activities whenever a specific model action (like saving or deleting) occurs—without tightly coupling that logic to your views or models.

### **Django ORM & Advanced ORM Techniques**
Django’s Object-Relational Mapper (ORM) allows developers to interact with the database using Python code instead of SQL. It also provides advanced tools to optimize performance—like `select_related`, `prefetch_related`, and query annotations—helping avoid common issues such as the N+1 query problem.

### **Basic Caching**
Caching stores frequently accessed data so it can be retrieved faster. Django supports various caching strategies (view-level, template fragment, low-level caching), which can drastically reduce page load time and database load.

Together, these techniques improve application responsiveness, database efficiency, and code scalability—making them crucial tools for Django backend developers.

---

## Learning Objectives
By the end of this module, learners will be able to:
- Explain and implement Django Signals to build event-driven features.
- Use Django ORM to perform CRUD operations and write efficient queries.
- Apply advanced ORM techniques for optimizing database access.
- Implement basic caching strategies to enhance performance.
- Follow best practices to ensure maintainable, decoupled, and performant backend code.

## Learning Outcomes
Learners will be able to:
- Use Django Signals to decouple side-effects from core business logic.
- Efficiently retrieve and manipulate database data using Django ORM.
- Avoid performance issues through query optimization techniques.
- Implement caching at the view, template, or data level to reduce server workload.
- Write clean and testable backend logic using Django’s built-in tools.

---

## 1. Event Listeners Using Django Signals
### **What are Signals?**
Django Signals allow certain senders to notify a set of receivers when specific actions have taken place. They’re useful for triggering side effects like notifications, logging, or updates across different parts of your application.

### **Common Signals:**
- `pre_save` / `post_save`
- `pre_delete` / `post_delete`
- `m2m_changed`
- `request_started` / `request_finished`

### **Best Practices:**
- Keep signal functions lean and avoid long-running tasks.
- Use the `@receiver` decorator to keep registration clean and explicit.
- Separate business logic from the signal handler—call a service or utility function.
- Disconnect signals during tests to prevent unwanted behavior.

---

## 2. Django ORM Basics
### 🔧 **What is ORM?**
The Object-Relational Mapper (ORM) allows interaction with the database using Python models instead of writing raw SQL. You can query, insert, update, and delete records using intuitive syntax.

### **Common Operations:**
- **Create:** `Model.objects.create(...)`
- **Retrieve:** `Model.objects.get(...)`, `.filter()`, `.all()`
- **Update:** `.save()`, `.update()`
- **Delete:** `.delete()`

### **Best Practices:**
- Always catch exceptions like `DoesNotExist` and `MultipleObjectsReturned`.
- Chain `.filter()` to narrow queries instead of retrieving all data.
- Validate data before saving.

---

## 3. Advanced ORM Techniques
### **Tools for Performance:**
- **`select_related()`** – optimizes foreign key lookups using SQL JOINs.
- **`prefetch_related()`** – optimizes many-to-many or reverse foreign key relations.
- **`annotate()`** – perform aggregations like counts, sums, averages, etc.
- **`Q` and `F` expressions** – for complex conditions and field-to-field calculations.
- **Custom Managers** – encapsulate reusable query logic.

### **Best Practices:**
- Avoid repeated queries with eager loading.
- Use `only()` or `defer()` to limit unnecessary field loading.
- Profile queries using Django Debug Toolbar or `.query`.

---

## 4. Basic Caching in Django
### **What is Caching?**
Caching stores the result of expensive computations or database queries to avoid reprocessing them. Django supports multiple levels of caching.

### **Common Tools:**
- `@cache_page(60 * 15)` – view-level caching.
- `{% cache 300 "sidebar" %}` – template fragment caching.
- `cache.set()`, `cache.get()` – low-level caching.

### **Best Practices:**
- Don’t cache sensitive or user-specific data unless scoped properly.
- Use cache versioning and meaningful keys.
- Invalidate or update cache on data change through signals or logic.

---

## 📝 Project Assessment (Hybrid)
Your project will be evaluated primarily through manual reviews. To ensure your full score, please:

- ✅ Complete your project on time  
- 📄 Submit all required files  
- 🔗 Generate your review link  
- 👥 Have your peers review your work  

An auto-check will also verify the presence of required core files.

---

## ⏰ Important Note
If the deadline passes, you won’t be able to generate your review link—so be sure to submit on time!

We’re here to support your learning journey. Happy coding! ✨

---

## Tasks

### **0. Implement Signals for User Notifications** (mandatory)
**Objective:** Automatically notify users when they receive a new message.

**Instructions:**
- Create a `Message` model with fields: sender, receiver, content, timestamp.
- Use Django **post_save** signal to trigger a notification when a message is created.
- Create a `Notification` model linked to both `User` and `Message`.
- Write a signal that listens for new messages and automatically creates a notification for the receiver.

**Repo:**
- GitHub repository: `alx-backend-python`
- Directory: `Django-signals_orm-0x04`
- Files: `messaging/models.py`, `messaging/signals.py`, `messaging/apps.py`, `messaging/admin.py`, `messaging/tests.py`

---

### **1. Create a Signal for Logging Message Edits** (mandatory)
**Objective:** Log when a user edits a

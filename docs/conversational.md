# conversational search

What is conversational search? 

keyword search -> semantic search -> conversational search powered by generative AI

what makes it different from traditional search: question understanding, conversation flow, answer generation.

it needs to remember the context of the entire conversation.

it is implemented with conversation history and RAG.

## conversation history

a simple CRUD API comprising two resources: memories and messages.

a message represents a question-answer pair: a human-input question and an AI answer.

all messages of the current conversation are added to a memory.

## RAG

RAG retrieves data from the index and conversation history and sends the result as context to the LLM.


## memory api

* AddMemory -> id
* PutMemory
* GetMemory
* GetAllMemories
* SearchMemory -> messages
* DeleteMemory

* AddMessage -> id
* PutMessage
* GetMesssage
* GetAllMessages


### system collections

memories and messages are stored in two system collections.



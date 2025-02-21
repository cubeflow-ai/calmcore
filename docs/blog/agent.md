# agent
## background
本项目主要演示基于calmserver的检索增强能力，以及如何通过calmserver的能力解决与大模型对话时可能碰到的典型问题。

## typical Q&A with RAG
基于calmserver实现一个简单的具备检索增强能力的问答服务，主要分为两个部分：
1、把用户的文档进行分块，并保存到calmserver，calmserver为这些文档块构建文本索引，为了方便后续进行语义检索，需要调用外部embedding model生成embedding vector并保存到calmserver。
2、根据用户的question检索相似的文档块，借助calmserver的混合检索能力，你可以同时进行文本和语义检索，然后把检索到的内容与question一起输入到大模型生成问题的答案。
### &emsp; indexing
### &emsp; retrieval & generation

## retrieval from more index space
用户的数据按照一定的分类被保存在多个index space中，用户的question可能涉及到多个index space，需要同时从这些index space中进行检索，然后通过一定的合并策略对检索到的文档块进行合并。
合并策略可以是：snake-merge、按relevant socre排序、指定index space优先等

## retrieval with reranking
基于文本相关性(bm25 .etc)、vector相似度（cosine-similarity .etc）检索到内容后，由于LLM的context window size有一定的限制，或者需要剔除一些不太相关的文档，这时需要对内容进行截断，由于model porformance或其他因素，可能排除了一些有用的文档内容，为了提升LLM 生成answer的准确性，可以引入reranking model对检索到的文档内容再进行一次排序，然后把TopN的内容送入大模型。

## multi-turn conversations
用与LLM对话过程中，往往不是一问一答后就能达到目标，LLM在没有获得足够多的信息对问题进行回答时，需要用户追加有用的信息，这个提供信息的过程可能会经过多次交互。
因此在做检索时，需要结合用户本轮对话的上下文信息进行检索，而不是只使用用户最后一次提问内容进行检索，同时使用LLM进行answer生成时，也需要结合本轮会话的上下文和检索到的内容进行生成。
### &emsp; query rewriting
	通过LLM，结合对话上下文信息，对用户最后一次的提问生成方便检索的查询词。查询词可能存在多组，需要分别进行检索然后对检索到的内容进行合并截断。

## long-term memory
LLM一般使用公开的或企业内部的资料进行训练，能够记住世界通用的知识，往往不会对用户个人信息进行记忆，同时当前的LLM比较难生成一些没有通用逻辑、是非类的信息。这些信息一般在对话过程中由用户提供，或者由用户以其他方式导入。
需要按用户的维度把上述信息保存起来，当下次用户提到相关问题时，可以像人类一样回忆起来这些内容，提供给LLM生成用户想要的答案或完成相关的任务。
### &emsp; 信息的更新
		以用户为例，用户可能会更换工作单位、喜好会变化，家庭成员会变化等
### &emsp; 相同信息的整合
		多次的聊天信息，可能提供了相同的内容，实际保存时需要避免重复
### &emsp; 信息的归类&联系
		TODO:maybe graph ???
## query Understanding
检索前，需要对query做纠错、意图识别、扩展等
TODO:
## task planning
用户提出的任务，可能无法直接完成，LLM需要规划一系列的子任务，然后通过多个子任务的协同最终完成用户提出的任务
TODO:

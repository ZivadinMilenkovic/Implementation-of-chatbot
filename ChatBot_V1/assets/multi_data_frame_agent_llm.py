from ..setup_the_agent_and_llm import setup_the_llm, setup_the_agent


class MultiDataFrameAgentLLM:
    def __init__(self, db):

        self.llm = setup_the_llm()
        self.db = db

    def run(self, question_from_the_user: str):

        agent = setup_the_agent(self.llm, self.db)

        answer = agent.invoke(question_from_the_user)
        print(answer)

        return answer["output"]

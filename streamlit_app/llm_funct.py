from openai import OpenAI




def get_insights(history):
    client = OpenAI()
    prompt = f"Analyze the following crypto trade summaries and summarize the trend:\n{history}"
    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}]
    )
    insights = response.choices[0].message.content
    return insights
 
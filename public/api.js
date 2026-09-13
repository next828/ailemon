export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: '仅支持POST请求' });
  }

  const { message, session_id } = req.body;

  if (!message) {
    return res.status(400).json({ error: '消息不能为空' });
  }

  const BOT_APP_KEY = process.env.BOT_APP_KEY;

  if (!BOT_APP_KEY) {
    return res.status(500).json({ error: '服务未配置，请联系管理员' });
  }

  try {
    const adpResponse = await fetch('https://wss.lke.cloud.tencent.com/v1/qbot/chat/sse', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: session_id || 'sess_' + Date.now(),
        bot_app_key: BOT_APP_KEY,
        visitor_biz_id: session_id || 'visitor_' + Date.now(),
        content: message,
        incremental: true,
        streaming_throttle: 10,
        visitor_labels: [],
        custom_variables: {},
        search_network: 'disable',
        stream: 'enable',
        workflow_status: 'disable',
        tcadp_user_id: ''
      })
    });

    if (!adpResponse.ok) {
      return res.status(502).json({ error: 'AI服务请求失败' });
    }

    // 设置SSE响应头
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const reader = adpResponse.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (line.startsWith('data:')) {
          const dataStr = line.slice(5).trim();
          if (!dataStr) continue;
          try {
            const data = JSON.parse(dataStr);
            if (data.type === 'reply' && data.content) {
              res.write(`data: ${JSON.stringify({ content: data.content })}\n\n`);
            }
          } catch (e) {}
        }
      }
    }

    res.write('data: [DONE]\n\n');
    res.end();
  } catch (err) {
    console.error('ADP调用错误:', err);
    res.status(500).json({ error: '服务内部错误' });
  }
}

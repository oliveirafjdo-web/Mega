Passos para deploy automático no Render

O repositório já contém `Procfile`, `requirements.txt` e `gunicorn_config.py`.

1) (Recomendado) Faça push do repositório para um Git remoto (GitHub/GitLab):

```bash
git add .
git commit -m "Prepare repo for Render"
git remote add origin <your-git-remote-url>
git push -u origin main
```

2) No painel do Render (https://render.com):
   - Clique em "New" → "Web Service".
   - Conecte sua conta GitHub/GitLab e selecione este repositório.
   - Para "Environment" escolha "Python".
   - Em "Build Command" use: `pip install -r requirements.txt`.
   - Em "Start Command" use: `gunicorn -c gunicorn_config.py app:app`.
       - Em "Start Command" use: `bash ./scripts/start.sh` (o `start.sh` copia `metrifiy.db` para o disco persistente quando necessário).
   - Habilite "Auto-Deploy" se quiser deploys automáticos ao dar push.

3) Variáveis de ambiente importantes (configure no painel do Render):
   - `DATABASE_URL` caso use PostgreSQL (Render fornece DB gerenciado opcional).
   - `SECRET_KEY` — valor aleatório para Flask sessions.
   - `PORT` não é obrigatório (Render define automaticamente), mas o `render.yaml` define `10000` por segurança.
   - `DATA_DIR` (opcional) — caminho onde o disco persistente é montado. Por padrão o script usa `/srv/data`.

4) Para manter o arquivo SQLite entre redeploys (RECOMENDADO):
   - Crie um Persistent Disk no painel do Render (Dashboard → Disks → Create Disk).
   - Anexe o Disk ao seu Web Service e monte-o em `/srv/data` (ou ajuste `DATA_DIR`).
   - O `scripts/start.sh` copia `metrifiy.db` do repositório para o disco na primeira execução.

Observação: o disco persistente é o método suportado para manter arquivos entre deploys. Se preferir um banco gerenciado, crie um PostgreSQL e configure `DATABASE_URL`.

4) Se preferir usar `render.yaml` (já adicionado), Render lerá o arquivo e criará o serviço conforme definido.

Limitações que eu não posso executar por você:
 - Criar a conta Render ou ligar o repositório (exigem autenticação/autorizações suas).
 - Fazer push para o seu Git remoto se eu não tiver credenciais.

Se quiser, posso:
 - Gerar um branch com ajustes adicionais (ex.: ajustar `requirements.txt`).
 - Gerar um script de deploy (`deploy_render.sh`) que você rode localmente para criar remote + push.

Se quiser, eu também posso criar um script `deploy_render.sh` que cria um remote Git e faz um push (você precisará confirmar a URL do remote).

Se desejar que eu gere o `deploy_render.sh`, responda "gerar script"; se preferir eu apenas atualize instruções, responda "apenas instruções".

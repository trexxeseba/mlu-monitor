# GitHub Actions Secrets Setup

Para que GitHub Actions pueda ejecutar el script, necesita 4 secrets:

## 1. GOOGLE_CREDENTIALS
El contenido COMPLETO del JSON de Google Service Account.

En tu terminal:
```bash
cat C:\Users\undia\mlu-monitor\clauditaaa-dbcde137b8d8.json
```

Copiá TODO lo que aparece (desde `{` hasta `}`)

## 2. SUPABASE_URL
```
https://drggfikyqtooqxqqwefy.supabase.co
```

## 3. SUPABASE_KEY
```
sb_secret_L5BFG8tcXPOc8qFhU7bCUg_FeFRH61W
```

## 4. SHEET_ID
```
1kU7f0vRsNVgcIF1wqyU4v1zopgTkfs8hcMewjT8teTE
```

---

## Cómo agregar los secrets a GitHub

1. Andá a tu repo: https://github.com/trexxeseba/mlu-monitor
2. **Settings** → **Secrets and variables** → **Actions**
3. Clickeá **"New repository secret"**
4. Agregá los 4 secrets arriba con esos NOMBRES exactos

Una vez hecho, GitHub Actions corre automáticamente cada 2 horas.

---

## Testear

En GitHub:
1. Andá a **Actions** en tu repo
2. Clickeá el workflow **"MLU Sheets Sync (every 2h)"**
3. Clickeá **"Run workflow"** → **"Run workflow"** (verde)

Debería ejecutarse en segundos y sincronizar los datos.

Si hay error, mirá los logs (clickear en el job que falló).

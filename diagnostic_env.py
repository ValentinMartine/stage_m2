"""
Script de diagnostic — à exécuter depuis le même endroit que analyse_ecbu.py
pour comprendre pourquoi le .env n'est pas chargé.
"""
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '.env')

print(f"1. Python utilisé    : {sys.executable}")
print(f"2. Répertoire courant: {os.getcwd()}")
print(f"3. SCRIPT_DIR        : {SCRIPT_DIR}")
print(f"4. ENV_PATH cherché  : {ENV_PATH}")
print(f"5. .env existe ?     : {os.path.isfile(ENV_PATH)}")

# Lister les fichiers du dossier pour vérifier le nom exact
print(f"\n6. Fichiers dans {SCRIPT_DIR} :")
for f in sorted(os.listdir(SCRIPT_DIR)):
    if f.startswith('.') or f == '.env' or 'env' in f.lower():
        print(f"   → {f!r}  (taille: {os.path.getsize(os.path.join(SCRIPT_DIR, f))} octets)")

# Tester python-dotenv
print(f"\n7. Test python-dotenv :")
try:
    from dotenv import load_dotenv
    print(f"   Module trouvé : oui")
    result = load_dotenv(ENV_PATH)
    print(f"   load_dotenv() retourne : {result}")
except ImportError:
    print(f"   Module trouvé : NON — installer avec: pip install python-dotenv")

# Vérifier la valeur
db_pass = os.getenv('DB_PASS', '')
print(f"\n8. DB_PASS après chargement : {'DÉFINI (' + str(len(db_pass)) + ' caractères)' if db_pass else 'VIDE'}")

# Lire le fichier manuellement si il existe
if os.path.isfile(ENV_PATH):
    print(f"\n9. Contenu brut du .env (masqué) :")
    with open(ENV_PATH, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            stripped = line.strip()
            if '=' in stripped and not stripped.startswith('#'):
                key, _, val = stripped.partition('=')
                print(f"   Ligne {i}: {key}={'*' * len(val)}")
            else:
                print(f"   Ligne {i}: {stripped[:50]}")
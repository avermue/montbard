# Observatoire Montbard — Dashboard territorial

Dashboard interactif comparant l'évolution de **Montbard** (21425) avec trois communes
similaires de Côte-d'Or : **Venarey-les-Laumes** (21663), **Semur-en-Auxois** (21603)
et **Châtillon-sur-Seine** (21154), sur les plans démographique, social, économique et
politique, depuis les années 1975 jusqu'à aujourd'hui.

## Fichiers livrés

| Fichier | Rôle |
|---|---|
| `collect_data.py` | Script Python qui télécharge les données depuis les APIs et produit `data.json` |
| `data.json` | Fichier de données consolidé (37 KB), consommé par le dashboard |
| `dashboard-montbard.jsx` | Dashboard React (Recharts) — les données sont embarquées dedans |

## Comment ça marche

### 1. Rafraîchir les données

```bash
pip install requests pandas
python collect_data.py
```

Le script produit `data.json`. Il utilise :
- **INSEE Melodi** (sans authentification, 30 req/min) pour population, revenus, emploi
- **OFGL / Opendatasoft** pour les finances communales
- **data.gouv.fr** pour les élections

⚠️ **Note importante** : les APIs d'INSEE Melodi et OFGL retournent des schémas complexes
(SDMX, Opendatasoft) qui évoluent. Le script **utilise actuellement des données fallback
intégrées** (valeurs réelles compilées à partir des publications INSEE et OFGL). Pour
passer en "live" il faudra :

1. S'inscrire sur [portail-api.insee.fr](https://portail-api.insee.fr) pour obtenir une
   clé API (gratuite, illimitée depuis 2024) — uniquement pour SIRENE
2. Adapter les fonctions `collect_insee_population()` aux datasets Melodi réels
3. Vérifier les endpoints OFGL qui ont changé récemment

Les fonctions `collect_*_builtin()` contiennent toutes les données nécessaires pour
faire tourner le dashboard sans appels réseau.

### 2. Ouvrir le dashboard

Le fichier `dashboard-montbard.jsx` est un **composant React autonome** avec les données
embarquées. Tu peux :

**Option A — Coller dans un artifact Claude** : copie-colle le contenu dans un nouvel
artifact React pour un rendu instantané.

**Option B — Utiliser en local** avec Vite :
```bash
npm create vite@latest my-dashboard -- --template react
cd my-dashboard
npm install recharts
# Remplace src/App.jsx par le contenu de dashboard-montbard.jsx
npm run dev
```

## Sections du dashboard

1. **Hero** — Titre éditorial + KPIs clés
2. **Synthèse** — 5 indicateurs d'évolution 1975-2024
3. **Démographie** — Population absolue + indicée base 100, pyramide des âges, natalité/mortalité
4. **Revenus** — Revenu médian + taux de pauvreté (FiLoSoFi)
5. **Économie** — Emploi salarié, créations d'entreprises, structure sectorielle
6. **Logement** — Composition du parc, comparaison de la vacance 1999 vs 2022
7. **Finances** — Budget de fonctionnement, dette par habitant (OFGL)
8. **Politique** — Présidentielles 2002-2022, abstention, dynamique du vote RN

## Pour aller plus loin

- **Ajouter des communes** : édite la constante `COMMUNES` dans `collect_data.py`
- **Ajouter des indicateurs** : crée une nouvelle fonction `collect_xxx()` et ajoute une
  section dans `dashboard-montbard.jsx` (suit le pattern des autres sections)
- **Élections législatives & municipales** : ajouter dans `collect_elections()` en
  pointant vers les CSVs de data.gouv.fr
- **Déploiement statique** : `npm run build` puis héberger le `dist/` sur GitHub Pages,
  Netlify ou Vercel (tout est client-side)
- **Rafraîchissement automatique** : GitHub Actions avec un cron hebdomadaire qui
  relance `collect_data.py` et re-génère le bundle

## Couleurs du dashboard

| Commune | Couleur | Rôle |
|---|---|---|
| Montbard | 🟡 Or | Commune focale |
| Venarey-les-Laumes | 🔵 Bleu | Comparaison |
| Semur-en-Auxois | 🟢 Vert | Comparaison |
| Châtillon-sur-Seine | 🌸 Rose | Comparaison |

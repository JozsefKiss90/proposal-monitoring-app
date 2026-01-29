## 1. Most Common and Useful Git Commands

### Repository Setup

| Command                       | Purpose                           |
| ----------------------------- | --------------------------------- |
| `git clone <url>`             | Copy a remote repository locally  |
| `git init`                    | Initialize a new local repository |
| `git remote -v`               | View configured remotes           |
| `git remote add origin <url>` | Add a remote repository           |

---

### Daily Work (Core Commands)

| Command                        | Purpose                             |
| ------------------------------ | ----------------------------------- |
| `git status`                   | Show working tree and staging state |
| `git add <file>` / `git add .` | Stage changes                       |
| `git commit -m "message"`      | Commit staged changes               |
| `git log`                      | View commit history                 |
| `git diff`                     | Show unstaged differences           |
| `git diff --staged`            | Show staged differences             |

---

### Branching

| Command                  | Purpose                    |
| ------------------------ | -------------------------- |
| `git branch`             | List branches              |
| `git branch <name>`      | Create branch              |
| `git checkout <branch>`  | Switch branches            |
| `git checkout -b <name>` | Create and switch          |
| `git switch <branch>`    | Modern alternative         |
| `git switch -c <name>`   | Create and switch (modern) |

---

### Syncing with Remote

| Command                       | Purpose                      |
| ----------------------------- | ---------------------------- |
| `git fetch`                   | Download remote changes only |
| `git pull`                    | Fetch + merge                |
| `git pull --rebase`           | Fetch + rebase               |
| `git push`                    | Upload commits to remote     |
| `git push -u origin <branch>` | Push and set upstream        |

---

### Merging & Cleanup

| Command                  | Purpose                   |
| ------------------------ | ------------------------- |
| `git merge <branch>`     | Merge branch into current |
| `git branch -d <branch>` | Delete merged branch      |
| `git branch -D <branch>` | Force delete branch       |

---

### Safety & Recovery (Very Useful)

| Command                   | Purpose                           |
| ------------------------- | --------------------------------- |
| `git stash`               | Temporarily save uncommitted work |
| `git stash pop`           | Restore stashed work              |
| `git reset --soft HEAD~1` | Undo commit, keep changes         |
| `git reset --hard HEAD~1` | Undo commit and changes           |
| `git reflog`              | Recover lost commits              |

---

## 2. Example Scenario: From New Repository to Merge

### Assumptions

* A new GitHub repository exists (empty or with README)
* Default branch is `main`
* You are working locally

---

### Step 1: Clone the New Repository

```bash
git clone https://github.com/org/example-repo.git
cd example-repo
```

---

### Step 2: Create Initial Commit (If Empty Repo)

```bash
echo "# Example Project" > README.md
git add README.md
git commit -m "Initial commit"
git push origin main
```

---

### Step 3: Create a Feature Branch

You never work directly on `main`.

```bash
git checkout -b feature/login
```

---

### Step 4: Make Changes and Commit

```bash
touch login.js
git add login.js
git commit -m "Add login module"
```

---

### Step 5: Push Feature Branch

```bash
git push -u origin feature/login
```

At this point:

* Changes exist on GitHub in `feature/login`
* `main` is unchanged

---

### Step 6: Open a Pull Request (GitHub UI)

* Base branch: `main`
* Compare branch: `feature/login`
* Review → approve → merge

(This step is manual in GitHub.)

---

### Step 7: Update Local `main` After Merge

Once the PR is merged on GitHub:

```bash
git checkout main
git pull origin main
```

Now your local `main` includes the merged changes.

---

### Step 8: Delete Feature Branch

```bash
git branch -d feature/login
```

(Optional remote cleanup)

```bash
git push origin --delete feature/login
```

---

## 3. Visual Flow (Conceptual)

```
main ──●───────────────●─────────
        \             /
feature  ●──●──●─────
```

* Work happens on `feature/*`
* Pull request merges into `main`
* `main` stays stable

---

## 4. Professional Rule Set (Recommended Defaults)

* Never commit directly to `main`
* Always branch for work
* Push early, push often
* Fetch or pull before starting work
* Merge via pull requests
* Delete branches after merge

---

Below is a realistic conflict scenario where a bad merge introduces a defect, and you must restore a previous working version using commit history. I will show two common recovery paths: **revert (preferred on shared branches)** and **reset (only when safe / private)**.

---

## Scenario Overview

* A new repository has a `main` branch.
* Two developers change the same function in `app.js` on different branches.
* The PR merge produces a conflict that is resolved incorrectly.
* The incorrect resolution is merged to `main`, breaking behavior.
* You restore the last known good version based on commit history.

---

## 1) Setup: Create a New Repo and Initial Commit

```bash
mkdir conflict-restore-demo
cd conflict-restore-demo
git init
git branch -M main
```

Create a file:

```bash
cat > app.js <<'EOF'
function greet(name) {
  return "Hello, " + name;
}

module.exports = { greet };
EOF
```

Commit and push (optional if you have a remote):

```bash
git add app.js
git commit -m "Initial greet implementation"
# git remote add origin <url>
# git push -u origin main
```

---

## 2) Create Two Branches That Modify the Same Lines

### Branch A: “feature/formal-greeting”

```bash
git checkout -b feature/formal-greeting
```

Edit `app.js`:

```bash
cat > app.js <<'EOF'
function greet(name) {
  return "Good day, " + name + ".";
}

module.exports = { greet };
EOF
```

Commit:

```bash
git add app.js
git commit -m "Use formal greeting"
```

### Branch B: “feature/null-safe”

```bash
git checkout main
git checkout -b feature/null-safe
```

Edit `app.js` differently:

```bash
cat > app.js <<'EOF'
function greet(name) {
  if (!name) return "Hello, stranger";
  return "Hello, " + name;
}

module.exports = { greet };
EOF
```

Commit:

```bash
git add app.js
git commit -m "Handle missing name safely"
```

---

## 3) Merge Branch A Into `main`

```bash
git checkout main
git merge feature/formal-greeting
```

Now `main` has “Good day, …”.

---

## 4) Merge Branch B Into `main` and Trigger a Conflict

```bash
git merge feature/null-safe
```

You should see a conflict in `app.js`.

Open `app.js` and you will see markers like:

```js
<<<<<<< HEAD
function greet(name) {
  return "Good day, " + name + ".";
}
=======
function greet(name) {
  if (!name) return "Hello, stranger";
  return "Hello, " + name;
}
>>>>>>> feature/null-safe
```

### Incorrect conflict resolution (introduces a bug)

Assume the resolver accidentally deletes the null check and keeps the formal greeting but breaks punctuation/logic:

```bash
cat > app.js <<'EOF'
function greet(name) {
  return "Good day, " + name; // BUG: no null handling, missing period, "name" can be undefined
}

module.exports = { greet };
EOF
```

Finish the merge:

```bash
git add app.js
git commit -m "Merge feature/null-safe into main (resolved conflict)"
```

At this point, **main is broken** (e.g., `greet()` returns `"Good day, undefined"`).

---

## 5) Identify the Last Known Good Commit Using History

Inspect the graph:

```bash
git log --oneline --graph --decorate --max-count=15
```

Example (your hashes will differ):

```
a9c2d41 (HEAD -> main) Merge feature/null-safe into main (resolved conflict)
7b1fa10 Use formal greeting
2c4e8b3 Handle missing name safely
9f8c123 Initial greet implementation
```

Here, the broken commit is `a9c2d41`. The last known good state on `main` was `7b1fa10` (or possibly earlier, depending on what you consider “good”).

---

# Recovery Option A (Recommended on Shared Branches): `git revert`

This creates a **new commit** that undoes the bad commit, preserving history (best for teams).

### Revert the bad merge commit

If the bad commit is a merge commit, revert needs the “mainline parent”:

```bash
git revert -m 1 a9c2d41
```

* `-m 1` means “treat parent #1 (main) as the mainline,” which is typical when reverting a merge into `main`.

Resolve any revert conflicts if prompted, then finish.

Push (if using remote):

```bash
git push origin main
```

**Result:** `main` is restored to the previous behavior, and the repository history remains intact.

---

# Recovery Option B (Only When Safe): `git reset --hard` + force push

This rewrites history. Do this only when:

* Nobody else has pulled the bad commit, or
* You explicitly coordinate with the team.

Reset `main` to last good commit:

```bash
git reset --hard 7b1fa10
```

If the bad commit is already on the remote, you would need:

```bash
git push --force-with-lease origin main
```

**Result:** `main` is exactly as it was at `7b1fa10`, as if the bad merge never happened—at the cost of rewriting shared history.

---

## How to “Confirm” the Restore Before Changing `main`

If you want to validate the old version before reverting/resetting, check it out temporarily:

```bash
git checkout 7b1fa10
# run tests / run app
git checkout main
```

This lets you confirm that commit is truly “known good.”

---

## Decision Rule

* Use **`git revert`** when the bad commit is already shared/pushed (most professional environments).
* Use **`git reset --hard`** only when you can safely rewrite history.


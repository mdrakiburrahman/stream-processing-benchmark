# Skills 🛠️

Each skill provides structured instructions for common development workflows.

## 📄 References

- [The Complete Guide to Building Skills](https://resources.anthropic.com/hubfs/The-Complete-Guide-to-Building-Skill-for-Claude.pdf)
- [Ralph Wiggum as a "software engineer"](https://ghuntley.com/ralph/)

## 🔁 Ralph Loop

We run a skill autonomously in a loop (re-invoking Copilot until the task is complete).

### How to

```bash
cd $(git rev-parse --show-toplevel)
./src/.scripts/ralph.sh .github/skills/ralph-spark-latency-tuning/skill.md -n 10
```

---

[Home](../../README.md) > [Skills](./)

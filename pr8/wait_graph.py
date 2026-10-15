#!/usr/bin/env python3
"""Single-instance resources; event syntax: acquire/wait/release PROCESS RESOURCE."""
import argparse
import json
from pathlib import Path


def find_cycle(graph):
    colors, stack, position = {}, [], {}

    def visit(node):
        colors[node] = 1
        position[node] = len(stack)
        stack.append(node)
        for neighbor in sorted(graph.get(node, ())):
            if colors.get(neighbor, 0) == 1:
                return stack[position[neighbor]:] + [neighbor]
            if colors.get(neighbor, 0) == 0:
                result = visit(neighbor)
                if result:
                    return result
        stack.pop()
        position.pop(node)
        colors[node] = 2
        return None

    nodes = set(graph)
    for neighbors in graph.values():
        nodes.update(neighbors)
    for node in sorted(nodes):
        if colors.get(node, 0) == 0:
            cycle = visit(node)
            if cycle:
                return cycle
    return None


class WaitGraph:
    def __init__(self):
        self.owners = {}       # resource -> process, exactly one instance per resource
        self.waiting = {}      # process -> resource, at most one outstanding wait

    def graph(self):
        graph = {}
        for process, resource in self.waiting.items():
            owner = self.owners.get(resource)
            if owner is not None:
                graph.setdefault(process, set()).add(owner)
        return graph

    def event(self, action, process, resource):
        if action == "acquire":
            if resource in self.owners:
                raise ValueError(f"{resource} already owned by {self.owners[resource]}")
            if process in self.waiting and self.waiting[process] != resource:
                raise ValueError("process acquired a different resource while waiting")
            self.owners[resource] = process
            self.waiting.pop(process, None)
        elif action == "wait":
            if resource not in self.owners:
                raise ValueError(f"cannot wait for free resource {resource}")
            if process in self.waiting:
                raise ValueError(f"{process} already has an outstanding wait")
            self.waiting[process] = resource
        elif action == "release":
            if self.owners.get(resource) != process:
                raise ValueError(f"{process} does not own {resource}")
            if process in self.waiting:
                raise ValueError("blocked process cannot release before its wait completes")
            del self.owners[resource]
        else:
            raise ValueError(f"unknown action: {action}")
        return find_cycle(self.graph())


def detect_file(path):
    state = WaitGraph()
    results = []
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        if len(fields) != 3:
            raise ValueError(f"line {line_number}: expected ACTION PROCESS RESOURCE")
        cycle = state.event(*fields)
        if cycle:
            results.append({"line": line_number, "cycle": cycle, "participants": cycle[:-1]})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file")
    args = parser.parse_args()
    results = detect_file(args.file)
    if results:
        for row in results:
            print(f"DEADLOCK at line {row['line']}: " + " -> ".join(row["cycle"]))
            print("Participants:", ", ".join(row["participants"]))
    else:
        print("NO DEADLOCK")


if __name__ == "__main__":
    main()

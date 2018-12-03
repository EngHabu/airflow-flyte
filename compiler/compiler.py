import datetime
import inspect
import os
import re

import six
from airflow.example_dags import example_bash_operator
from airflow.models import TaskInstance
from flyteidl.core.tasks_pb2 import SingleStepTask
from flytekit.common import interface as interface_common
from flytekit.common import promise as promise_common, workflow as workflow_common
from flytekit.common.tasks import task as base_tasks
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import interface as interface_model, task as task_model
from flytekit.models import literals as literals_model
from flytekit.models.workflow_closure import WorkflowClosure
from flytekit.sdk.types import Types

from airflow import DAG


class FlyteCompiler(object):
    """DSL Compiler.

    It compiles airflow dag into Flyte Workflow Template. Example usage:
    ```python

    FlyteCompiler().compile(my_dag, 'path/to/workflow.yaml')
    ```
    """

    def _op_to_task(self, dag_id, image, op, node_map):
        """
        Generate task given an operator inherited from dsl.ContainerOp.

        :param airflow.models.BaseOperator op:
        :param dict(Text, SdkNode) node_map:
        :rtype: Tuple(base_tasks.SdkTask, SdkNode)
        """

        interface_inputs = {}
        interface_outputs = {}
        input_mappings = {}
        processed_args = None

        # for key, val in six.iteritems(op.params):
        #     interface_inputs[key] = interface_model.Variable(
        #         _type_helpers.python_std_to_sdk_type(Types.String).to_flyte_literal_type(),
        #         ''
        #     )
        #
        #     if param.op_name == '':
        #         binding = promise_common.Input(sdk_type=Types.String, name=param.name)
        #     else:
        #         binding = promise_common.NodeOutput(
        #             sdk_node=node_map[param.op_name],
        #             sdk_type=Types.String,
        #             var=param.name)
        #     input_mappings[param.name] = binding
        #
        # for param in op.outputs.values():
        #     interface_outputs[param.name] = interface_model.Variable(
        #         _type_helpers.python_std_to_sdk_type(Types.String).to_flyte_literal_type(),
        #         ''
        #     )

        requests = []
        if op.resources:
            requests.append(
                task_model.Resources.ResourceEntry(
                    task_model.Resources.ResourceName.Cpu,
                    op.resources.cpus
                )
            )

            requests.append(
                task_model.Resources.ResourceEntry(
                    task_model.Resources.ResourceName.Memory,
                    op.resources.ram
                )
            )

            requests.append(
                task_model.Resources.ResourceEntry(
                    task_model.Resources.ResourceName.Gpu,
                    op.resources.gpus
                )
            )

            requests.append(
                task_model.Resources.ResourceEntry(
                    task_model.Resources.ResourceName.Storage,
                    op.resources.disk
                )
            )

        task_instance = TaskInstance(op, datetime.datetime.now())
        command = task_instance.command_as_list(
            local=True,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=True,
            ignore_task_deps=True,
            ignore_ti_state=True,
            pool=task_instance.pool,
            pickle_id=dag_id,
            cfg_path=None)

        task = base_tasks.SdkTask(
            op.task_id,
            SingleStepTask,
            "airflow_op",
            task_model.TaskMetadata(
                False,
                task_model.RuntimeMetadata(
                    type=task_model.RuntimeMetadata.RuntimeType.Other,
                    version=airflow.version.version,
                    flavor='airflow'
                ),
                datetime.timedelta(seconds=0),
                literals_model.RetryStrategy(0),
                '1',
                None,
            ),
            interface_common.TypedInterface(inputs=interface_inputs, outputs=interface_outputs),
            custom=None,
            container=task_model.Container(
                image=image,
                command=command,
                args=[],
                resources=task_model.Resources(limits=[], requests=requests),
                env={},
                config={},
            )
        )

        return task, task(**input_mappings).assign_id_and_return(op.task_id)

    def _create_tasks(self, tasks):
        """

        :param list[airflow.models.BaseOperator] tasks:
        :rtype: Tuple(list[flytekit.common.tasks.SdkTask], list[SdkNode])
        """
        tasks = set()
        nodes = {}
        for op in tasks:
            task, node = self._op_to_task(op, nodes)
            tasks.add(task)
            nodes[node.id] = node
        return tasks, [v for k, v in six.iteritems(nodes)]

    def _create_workflow(self, name, tasks):
        """
        Create workflow for the pipeline.

        :param str name:
        :param list[airflow.models.BaseOperator] tasks:
        """

        deps = {}
        for t in tasks:
            deps[t] = t.upstream_task_ids

        tasks, nodes = self._create_tasks(tasks)

        # Create map to look up tasks by their fully-qualified name. This map goes from something like
        # app.workflows.MyWorkflow.task_one to the task_one SdkRunnable task object
        tmap = {}
        for t in tasks:
            # This mocks an Admin registration, setting the reference id to the name of the task itself
            t.target._reference_id = t.id
            tmap[t.id] = t

        w = workflow_common.SdkWorkflow(inputs=[], outputs=[], nodes=nodes)
        task_templates = []
        for n in w.nodes:
            if n.task_node is not None:
                task_templates.append(tmap[n.task_node.reference_id])
            # TODO: sub_dags should be converted to subwokflows
            # elif n.workflow_node is not None:
            #     n.workflow_node._launchplan_ref = n.workflow_node.id
            #     n.workflow_node._sub_workflow_ref = n.workflow_node.id
            if n.id in deps:
                n._upstream_node_ids = deps[n.id]

        # Create the WorkflowClosure object that wraps both the workflow and its tasks
        return WorkflowClosure(workflow=w, tasks=task_templates)

    def _compile(self, dag):
        """

        :param airflow.DAG dag:
        :rtype: flytekit.models.workflow_closure
        """

        return self._create_workflow(dag.dag_id, dag.tasks)

    def compile(self, dag, path):
        """Compile the given pipeline function into workflow yaml.

        Args:
          dag: pipeline functions with @dsl.pipeline decorator.
          path: the output workflow tar.gz file path. for example, "~/a.tar.gz"
        """
        workflow = self._compile(dag)
        file_name = os.path.join(path, '{}.pb'.format(workflow.workflow.id))
        with open(file_name, 'wb') as fd:
            fd.write(workflow.to_flyte_idl().SerializeToString())
        print(file_name)


if __name__ == "__main__":
    # dag.cli()
    dag = example_bash_operator.dag
    print(FlyteCompiler()._compile(dag))

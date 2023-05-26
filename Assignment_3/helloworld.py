from clearml.automation.controller import PipelineDecorator

@PipelineDecorator.component(return_values=["x"], cache=True)
def step_one():
    print('Шаг 1')
    return 5

@PipelineDecorator.component(return_values=["y"], cache=True)
def step_two(x):
    print("Шаг 2")
    return x+1

@PipelineDecorator.pipeline(name='test', project='tests', version='0.0.1')
def executing_pipeline():
    x = step_one()
    y = step_two(x)
    print(y)

if __name__ == '__main__':
    # PipelineDecorator.set_default_execution_queue("defaults")
    PipelineDecorator.run_locally()
    executing_pipeline()
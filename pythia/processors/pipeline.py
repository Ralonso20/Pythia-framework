"""
Pipeline Processor - Multi-stage message processing pipeline
"""

import asyncio
from typing import Any, Callable, Awaitable, List, Optional
from pythia.core.message import Message
from pythia.logging.setup import get_pythia_logger


class PipelineStage:
    """Single stage in a processing pipeline"""

    def __init__(
        self,
        name: str,
        process_func: Callable[[Any], Awaitable[Any]],
        error_handler: Optional[Callable[[Any, Exception], Awaitable[Any]]] = None,
        parallel: bool = False,
    ):
        """
        Initialize pipeline stage

        Args:
            name: Stage name
            process_func: Processing function
            error_handler: Optional error handler
            parallel: Whether this stage can process in parallel
        """
        self.name = name
        self.process_func = process_func
        self.error_handler = error_handler
        self.parallel = parallel
        self.processed_count = 0
        self.error_count = 0

    async def process(self, data: Any) -> Any:
        """Process data through this stage"""
        try:
            result = await self.process_func(data)
            self.processed_count += 1
            return result

        except Exception as e:
            self.error_count += 1

            if self.error_handler:
                return await self.error_handler(data, e)
            raise

    def get_stats(self) -> dict:
        """Get stage statistics"""
        return {
            "name": self.name,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "parallel": self.parallel,
        }


class PipelineProcessor:
    """
    Multi-stage pipeline processor for complex message processing

    Example:
        # Stage 1: Parse message
        async def parse_message(message):
            return json.loads(message.body)

        # Stage 2: Validate data
        async def validate_data(data):
            if 'required_field' not in data:
                raise ValueError("Missing required field")
            return data

        # Stage 3: Transform data
        async def transform_data(data):
            return {'transformed': data}

        pipeline = PipelineProcessor([
            PipelineStage("parse", parse_message),
            PipelineStage("validate", validate_data),
            PipelineStage("transform", transform_data, parallel=True)
        ])

        result = await pipeline.process(message)
    """

    def __init__(
        self,
        stages: List[PipelineStage],
        name: str = "Pipeline",
        max_parallel: int = 10,
    ):
        """
        Initialize pipeline processor

        Args:
            stages: List of pipeline stages
            name: Pipeline name
            max_parallel: Maximum parallel tasks for parallel stages
        """
        self.stages = stages
        self.name = name
        self.max_parallel = max_parallel
        self.logger = get_pythia_logger(f"PipelineProcessor[{name}]")

        # Stats
        self.processed_count = 0
        self.error_count = 0
        self._semaphore = asyncio.Semaphore(max_parallel)

    async def process(self, message: Message) -> Any:
        """Process message through the entire pipeline"""
        try:
            self.logger.debug(
                "Starting pipeline processing",
                message_id=message.message_id,
                stages=len(self.stages),
            )

            result = message

            for i, stage in enumerate(self.stages):
                try:
                    self.logger.debug(
                        f"Processing stage {i + 1}/{len(self.stages)}",
                        stage=stage.name,
                        message_id=message.message_id,
                    )

                    if stage.parallel and isinstance(result, list):
                        # Process list items in parallel
                        result = await self._process_parallel(stage, result)
                    else:
                        # Process sequentially
                        result = await stage.process(result)

                    self.logger.debug(
                        f"Completed stage {i + 1}/{len(self.stages)}",
                        stage=stage.name,
                        message_id=message.message_id,
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error in stage {i + 1}",
                        stage=stage.name,
                        error=str(e),
                        message_id=message.message_id,
                    )
                    raise

            self.processed_count += 1
            self.logger.debug("Pipeline processing completed", message_id=message.message_id)
            return result

        except Exception as e:
            self.error_count += 1
            self.logger.error(
                "Pipeline processing failed",
                error=str(e),
                message_id=message.message_id,
            )
            raise

    async def process_batch(self, messages: List[Message]) -> List[Any]:
        """Process a batch of messages through the pipeline"""
        tasks = [self.process(message) for message in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error("Batch processing error", error=str(result))
                processed_results.append(None)  # or handle differently
            else:
                processed_results.append(result)

        return processed_results

    async def _process_parallel(self, stage: PipelineStage, items: List[Any]) -> List[Any]:
        """Process items in parallel through a stage"""

        async def process_item(item):
            async with self._semaphore:
                return await stage.process(item)

        tasks = [process_item(item) for item in items]
        return await asyncio.gather(*tasks)

    def add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline"""
        self.stages.append(stage)

    def remove_stage(self, stage_name: str) -> bool:
        """Remove a stage from the pipeline"""
        for i, stage in enumerate(self.stages):
            if stage.name == stage_name:
                del self.stages[i]
                return True
        return False

    def get_stage(self, stage_name: str) -> Optional[PipelineStage]:
        """Get a stage by name"""
        for stage in self.stages:
            if stage.name == stage_name:
                return stage
        return None

    def get_stats(self) -> dict:
        """Get pipeline statistics"""
        stage_stats = [stage.get_stats() for stage in self.stages]

        return {
            "name": self.name,
            "type": "pipeline",
            "stages": len(self.stages),
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "max_parallel": self.max_parallel,
            "success_rate": (
                self.processed_count / (self.processed_count + self.error_count) * 100
                if (self.processed_count + self.error_count) > 0
                else 0
            ),
            "stage_stats": stage_stats,
        }

    def reset_stats(self) -> None:
        """Reset pipeline statistics"""
        self.processed_count = 0
        self.error_count = 0
        for stage in self.stages:
            stage.processed_count = 0
            stage.error_count = 0

    def __repr__(self) -> str:
        stage_names = [stage.name for stage in self.stages]
        return f"PipelineProcessor(name={self.name}, stages={stage_names})"

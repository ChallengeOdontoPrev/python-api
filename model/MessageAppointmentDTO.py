from pydantic import BaseModel


class MessageAppointmentValidationDTO(BaseModel):
    idAppointment: int
    imgUrlInitial: str
    imgUrlFinal: str
    classInitial: str
    classFinal: str
